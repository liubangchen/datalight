namespace datalight::trino::protocol {
void to_json(json& j, const std::shared_ptr<ColumnHandle>& p) {
  if (p == nullptr) {
    return;
  }
  String type = p->_type;

  if (getConnectorKey(type) == "hive") {
    j = *std::static_pointer_cast<HiveColumnHandle>(p);
    return;
  }

  throw TypeError(type + " no abstract type ColumnHandle ");
}

void from_json(const json& j, std::shared_ptr<ColumnHandle>& p) {
  String type;
  try {
    type = p->getSubclassKey(j);
  } catch (json::parse_error& e) {
    throw ParseError(std::string(e.what()) + " ColumnHandle  ColumnHandle");
  }

  if (getConnectorKey(type) == "hive") {
    std::shared_ptr<HiveColumnHandle> k = std::make_shared<HiveColumnHandle>();
    j.get_to(*k);
    p = std::static_pointer_cast<ColumnHandle>(k);
    return;
  }

  throw TypeError(type + " no abstract type ColumnHandle ");
}
}
