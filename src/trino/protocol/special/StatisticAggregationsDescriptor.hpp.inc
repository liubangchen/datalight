namespace datalight::protocol {
    struct ColumnStatisticMetadata;
    template <typename T>
        struct StatisticAggregationsDescriptor {
            std::shared_ptr<Map<String, T>> grouping ={};
            std::shared_ptr<Map<TableStatisticType, T>> tableStatistics={};
            std::shared_ptr<Map<ColumnStatisticMetadata, T>> columnStatistics={};
        };

}

namespace nlohmann {

       template <typename T>  struct adl_serializer<datalight::protocol::StatisticAggregationsDescriptor<T>> {

        static void to_json(
            json& j,
            const datalight::protocol:: StatisticAggregationsDescriptor<T>& p) {

        }

        static void from_json(
            const json& j,
            datalight::protocol:: StatisticAggregationsDescriptor<T>& p) {

        }
       };
}
