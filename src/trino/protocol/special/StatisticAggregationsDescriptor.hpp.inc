namespace datalight::protocol {

    template <typename T>
        struct StatisticAggregationsDescriptor {
            std::shared_ptr<Map<String, T>> grouping ={};
            std::shared_ptr<Map<TableStatisticType, T>> tableStatistics={};
            std::shared_ptr<Map<ColumnStatisticMetadata, T>> columnStatistics={};
        };

    void to_json(json& j, const StatisticAggregationsDescriptor& p);
    void from_json(const json& j, StatisticAggregationsDescriptor& p);

}
