#define DUCKDB_EXTENSION_MAIN

#include "iceberg_extension.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction_manager.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "iceberg_functions.hpp"
#include "catalog_api.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/authorization/sigv4.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

static unique_ptr<TransactionManager> CreateTransactionManager(StorageExtensionInfo *storage_info, AttachedDatabase &db,
                                                               Catalog &catalog) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	return make_uniq<ICTransactionManager>(db, ic_catalog);
}

class IRCStorageExtension : public StorageExtension {
public:
	IRCStorageExtension() {
		attach = IRCatalog::Attach;
		create_transaction_manager = CreateTransactionManager;
	}
};

vector<SecretType> GetSecretType() {
	vector<SecretType> res;

	SecretType s;
	s.name = "BF_EDS";
	s.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	s.default_provider = "config";
	res.push_back(move(s));

	return res;
}

unique_ptr<BaseSecret> CreateBFEDSSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	// Set fields
	secret->TrySetValue("k1", input);
	secret->TrySetValue("k2", input);
	secret->TrySetValue("bf_aes_k", input);

	return std::move(secret);
}

vector<CreateSecretFunction> GetSecretFunction() {
	vector<CreateSecretFunction> res;

	CreateSecretFunction config_fun;
	config_fun.secret_type = "BF_EDS";
	config_fun.provider = "config";
	config_fun.function = CreateBFEDSSecretFromConfig;
	config_fun.named_parameters["k1"] = LogicalType::VARCHAR;
	config_fun.named_parameters["k2"] = LogicalType::VARCHAR;
	config_fun.named_parameters["bf_aes_k"] = LogicalType::VARCHAR;
	res.push_back(move(config_fun));

	return res;
}

static void LoadInternal(DatabaseInstance &instance) {
	ExtensionHelper::AutoLoadExtension(instance, "parquet");
	ExtensionHelper::AutoLoadExtension(instance, "avro");
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The iceberg extension requires the parquet extension to be loaded!");
	}

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("unsafe_enable_version_guessing",
	                          "Enable globbing the filesystem (if possible) to find the latest version metadata. This "
	                          "could result in reading an uncommitted version.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("use_encrypted_bloom_filters",
							  "Use encrypted bloom filters for range queries.",
							  LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("bloom_filter_encryption_method",
							  "The method used to encrypt the bloom filters. One of 'none', 'aes' or 'xor'.",
							  LogicalType::VARCHAR, Value("none"));
	config.AddExtensionOption("bloom_filter_m",
							  "The size of the bloom filter bitset.",
							  LogicalType::INTEGER, Value::INTEGER(8192));
	config.AddExtensionOption("write_to_file",
							  "Whether to skip the reading of Parquet files, and instead write the to-be-queried files to disk.",
							  LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("file_name",
	                          "The filename to write the Parquet file list to.",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption("skip_reading_parquet",
							  "Whether to skip reading the actual Parquet files.",
							  LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Iceberg Table Functions
	for (auto &fun : IcebergFunctions::GetTableFunctions(instance)) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	// Iceberg Scalar Functions
	for (auto &fun : IcebergFunctions::GetScalarFunctions()) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	SecretType secret_type;
	secret_type.name = "iceberg";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);
	CreateSecretFunction secret_function = {"iceberg", "config", OAuth2Authorization::CreateCatalogSecretFunction};
	OAuth2Authorization::SetCatalogSecretParameters(secret_function);
	ExtensionUtil::RegisterFunction(instance, secret_function);

	auto &log_manager = instance.GetLogManager();
	log_manager.RegisterLogType(make_uniq<IcebergLogType>());

	config.storage_extensions["iceberg"] = make_uniq<IRCStorageExtension>();

	ExtensionUtil::RegisterSecretType(instance, GetSecretType()[0]);
	ExtensionUtil::RegisterFunction(instance, GetSecretFunction()[0]);
}

void IcebergExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
string IcebergExtension::Name() {
	return "iceberg";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void iceberg_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *iceberg_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
