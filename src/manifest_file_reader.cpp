#include "manifest_reader.hpp"

namespace duckdb {

ManifestFileReader::ManifestFileReader(idx_t iceberg_version, bool skip_deleted)
    : BaseManifestReader(iceberg_version), skip_deleted(skip_deleted) {
}

void ManifestFileReader::SetSequenceNumber(sequence_number_t sequence_number_p) {
	sequence_number = sequence_number_p;
}

void ManifestFileReader::SetPartitionSpecID(int32_t partition_spec_id_p) {
	partition_spec_id = partition_spec_id_p;
}

idx_t ManifestFileReader::Read(idx_t count, vector<IcebergManifestEntry> &result) {
	if (!scan || finished) {
		return 0;
	}

	idx_t total_read = 0;
	idx_t total_added = 0;
	while (total_read < count && !finished) {
		auto tuples = ScanInternal(count - total_read);
		if (finished) {
			break;
		}
		total_added += ReadChunk(offset, tuples, result);
		offset += tuples;
		total_read += tuples;
	}
	return total_added;
}

void ManifestFileReader::CreateNameMapping(idx_t column_id, const LogicalType &type, const string &name) {
	auto lname = StringUtil::Lower(name);
	if (lname != "data_file") {
		name_to_vec[lname] = ColumnIndex(column_id);
		return;
	}

	if (type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("The 'data_file' of the manifest should be a STRUCT");
	}
	auto &children = StructType::GetChildTypes(type);
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		auto child_name = StringUtil::Lower(child.first);

		name_to_vec[child_name] = ColumnIndex(column_id, {ColumnIndex(child_idx)});
	}
}

bool ManifestFileReader::ValidateNameMapping() {
	if (!name_to_vec.count("status")) {
		return false;
	}
	if (!name_to_vec.count("file_path")) {
		return false;
	}
	if (!name_to_vec.count("file_format")) {
		return false;
	}
	if (!name_to_vec.count("record_count")) {
		return false;
	}
	if (iceberg_version > 1) {
		if (!name_to_vec.count("content")) {
			return false;
		}
	}
	return true;
}

static unordered_map<int32_t, Value> GetBounds(Vector &bounds, idx_t index) {
	auto &bounds_child = ListVector::GetEntry(bounds);
	auto keys = FlatVector::GetData<int32_t>(*StructVector::GetEntries(bounds_child)[0]);
	auto &values = *StructVector::GetEntries(bounds_child)[1];
	auto bounds_list = FlatVector::GetData<list_entry_t>(bounds);

	unordered_map<int32_t, Value> parsed_bounds;

	auto &validity = FlatVector::Validity(bounds);
	if (!validity.RowIsValid(index)) {
		return parsed_bounds;
	}

	auto list_entry = bounds_list[index];
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		parsed_bounds[keys[list_idx]] = values.GetValue(list_idx);
	}
	return parsed_bounds;
}

static unordered_map<int32_t, vector<uint8_t>> GetBloomFilters(Vector &bloom_filters, idx_t index) {
	auto &bloom_filters_child = ListVector::GetEntry(bloom_filters);
	auto keys = FlatVector::GetData<int32_t>(*StructVector::GetEntries(bloom_filters_child)[0]);
	auto values = FlatVector::GetData<string_t>(*StructVector::GetEntries(bloom_filters_child)[1]);
	auto bloom_filters_list = FlatVector::GetData<list_entry_t>(bloom_filters);

	unordered_map<int32_t, vector<uint8_t>> parsed_bloom_filters;

	auto &validity = FlatVector::Validity(bloom_filters);
	if (!validity.RowIsValid(index)) {
		return parsed_bloom_filters;
	}

	auto list_entry = bloom_filters_list[index];
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;

		// TODO: Improve this (zero copy possible?)
		vector<uint8_t> vec;
		string s = values[list_idx].GetString();
		vec.assign(s.begin(), s.end());
		parsed_bloom_filters[keys[list_idx]] = vec;
	}

	return parsed_bloom_filters;
}

static unordered_map<int32_t, int64_t> GetCounts(Vector &counts, idx_t index) {
	auto &counts_child = ListVector::GetEntry(counts);
	auto keys = FlatVector::GetData<int32_t>(*StructVector::GetEntries(counts_child)[0]);
	auto values = FlatVector::GetData<int64_t>(*StructVector::GetEntries(counts_child)[1]);
	auto counts_list = FlatVector::GetData<list_entry_t>(counts);

	unordered_map<int32_t, int64_t> parsed_counts;

	auto &validity = FlatVector::Validity(counts);
	if (!validity.RowIsValid(index)) {
		return parsed_counts;
	}

	auto list_entry = counts_list[index];
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		parsed_counts[keys[list_idx]] = values[list_idx];
	}
	return parsed_counts;
}

static vector<int32_t> GetEqualityIds(Vector &equality_ids, idx_t index) {
	vector<int32_t> result;

	if (!FlatVector::Validity(equality_ids).RowIsValid(index)) {
		return result;
	}
	auto &equality_ids_child = ListVector::GetEntry(equality_ids);
	auto equality_ids_data = FlatVector::GetData<int32_t>(equality_ids_child);
	auto equality_ids_list = FlatVector::GetData<list_entry_t>(equality_ids);
	auto list_entry = equality_ids_list[index];

	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		result.push_back(equality_ids_data[list_idx]);
	}

	return result;
}

idx_t ManifestFileReader::ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result) {
	D_ASSERT(offset < chunk.size());
	D_ASSERT(offset + count <= chunk.size());

	auto status = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("status").GetPrimaryIndex()]);

	auto file_path_idx = name_to_vec.at("file_path");
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(chunk.data[data_file_idx]);
	D_ASSERT(name_to_vec.at("file_format").GetPrimaryIndex() == data_file_idx);
	D_ASSERT(name_to_vec.at("record_count").GetPrimaryIndex() == data_file_idx);
	if (iceberg_version > 1) {
		D_ASSERT(name_to_vec.at("content").GetPrimaryIndex() == data_file_idx);
	}
	optional_ptr<Vector> equality_ids;
	optional_ptr<Vector> sequence_number;
	int32_t *content;

	auto partition_idx = name_to_vec.at("partition");
	if (iceberg_version > 1) {
		auto equality_ids_it = name_to_vec.find("equality_ids");
		if (equality_ids_it != name_to_vec.end()) {
			equality_ids = *child_entries[equality_ids_it->second.GetChildIndex(0).GetPrimaryIndex()];
		}
		auto sequence_number_it = name_to_vec.find("sequence_number");
		if (sequence_number_it != name_to_vec.end()) {
			sequence_number = chunk.data[sequence_number_it->second.GetPrimaryIndex()];
		}
		content =
		    FlatVector::GetData<int32_t>(*child_entries[name_to_vec.at("content").GetChildIndex(0).GetPrimaryIndex()]);
	}

	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format =
	    FlatVector::GetData<string_t>(*child_entries[name_to_vec.at("file_format").GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count =
	    FlatVector::GetData<int64_t>(*child_entries[name_to_vec.at("record_count").GetChildIndex(0).GetPrimaryIndex()]);
	auto file_size_in_bytes = FlatVector::GetData<int64_t>(
	    *child_entries[name_to_vec.at("file_size_in_bytes").GetChildIndex(0).GetPrimaryIndex()]);
	optional_ptr<Vector> lower_bounds;
	optional_ptr<Vector> upper_bounds;
	optional_ptr<Vector> value_counts;
	optional_ptr<Vector> null_value_counts;
	optional_ptr<Vector> nan_value_counts;
	optional_ptr<Vector> bloom_filters;

	auto lower_bounds_it = name_to_vec.find("lower_bounds");
	if (lower_bounds_it != name_to_vec.end()) {
		lower_bounds = *child_entries[lower_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto upper_bounds_it = name_to_vec.find("upper_bounds");
	if (upper_bounds_it != name_to_vec.end()) {
		upper_bounds = *child_entries[upper_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto bloom_filters_it = name_to_vec.find("bloom_filters");
	if (bloom_filters_it != name_to_vec.end()) {
		bloom_filters = *child_entries[bloom_filters_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto value_counts_it = name_to_vec.find("value_counts");
	if (value_counts_it != name_to_vec.end()) {
		value_counts = *child_entries[value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto null_value_counts_it = name_to_vec.find("null_value_counts");
	if (null_value_counts_it != name_to_vec.end()) {
		null_value_counts = *child_entries[null_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto nan_value_counts_it = name_to_vec.find("nan_value_counts");
	if (nan_value_counts_it != name_to_vec.end()) {
		nan_value_counts = *child_entries[nan_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto &partition_vec = child_entries[partition_idx.GetChildIndex(0).GetPrimaryIndex()];

	idx_t produced = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[index];
		if (this->skip_deleted && entry.status == IcebergManifestEntryStatusType::DELETED) {
			//! Skip this entry, we don't care about deleted entries
			continue;
		}

		entry.file_path = file_path[index].GetString();
		entry.file_format = file_format[index].GetString();
		entry.record_count = record_count[index];
		entry.file_size_in_bytes = file_size_in_bytes[index];

		if (lower_bounds && upper_bounds) {
			entry.lower_bounds = GetBounds(*lower_bounds, index);
			entry.upper_bounds = GetBounds(*upper_bounds, index);
		}
		if (bloom_filters) {
			entry.bloom_filters = GetBloomFilters(*bloom_filters, index);
		}
		if (value_counts) {
			entry.value_counts = GetCounts(*value_counts, index);
		}
		if (null_value_counts) {
			entry.null_value_counts = GetCounts(*null_value_counts, index);
		}
		if (nan_value_counts) {
			entry.nan_value_counts = GetCounts(*nan_value_counts, index);
		}

		if (iceberg_version > 1) {
			entry.content = (IcebergManifestEntryContentType)content[index];
			if (equality_ids) {
				entry.equality_ids = GetEqualityIds(*equality_ids, index);
			}

			if (sequence_number) {
				auto sequence_numbers = FlatVector::GetData<int64_t>(*sequence_number);
				if (FlatVector::Validity(*sequence_number).RowIsValid(index)) {
					entry.sequence_number = sequence_numbers[index];
				} else {
					//! Value should only be NULL for ADDED manifest entries, to support inheritance
//					D_ASSERT(entry.status == IcebergManifestEntryStatusType::ADDED);
					entry.sequence_number = this->sequence_number;
				}
			} else {
				//! Default to sequence number 0
				//! (The 'manifest_file' should also have defaulted to 0)
				D_ASSERT(this->sequence_number == 0);
				entry.sequence_number = 0;
			}
		} else {
			entry.sequence_number = this->sequence_number;
			entry.content = IcebergManifestEntryContentType::DATA;
		}

		entry.partition_spec_id = this->partition_spec_id;
		entry.partition = partition_vec->GetValue(index);
		produced++;
		result.push_back(entry);
	}
	return produced;
}

} // namespace duckdb
