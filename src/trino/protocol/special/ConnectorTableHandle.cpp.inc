namespace datalight::trino::protocol {
void to_json(json& j, const std::shared_ptr<ConnectorTableHandle>& p) {
  if (p == nullptr) {
    return;
  }
  String type = p->_type;

  if (getConnectorKey(type) == "hive") {
    j = *std::static_pointer_cast<HiveTableHandle>(p);
    return;
  }

  throw TypeError(type + " no abstract type ConnectorTableHandle");
}

void from_json(const json& j, std::shared_ptr<ConnectorTableHandle>& p) {
  String type;
  try {
    type = p->getSubclassKey(j);
  } catch (json::parse_error& e) {
    throw ParseError(
        std::string(e.what()) + " ConnectorTableHandle  ConnectorTableHandle");
  }

  if (getConnectorKey(type) == "hive") {
    auto k = std::make_shared<HiveTableHandle>();
    j.get_to(*k);
    p = k;
    return;
  }

  throw TypeError(type + " no abstract type ConnectorTableHandle");
}
}
