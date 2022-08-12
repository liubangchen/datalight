namespace datalight::trino::protocol {

    struct CallExpression : RowExpression {
        ResolvedFunction resolvedFunction;
        List<RowExpression> arguments;
    };

    void to_json(json& j, const CallExpression& p);
    void from_json(const json& j, CallExpression& p);

}// namespace datalight::trino::protocol
