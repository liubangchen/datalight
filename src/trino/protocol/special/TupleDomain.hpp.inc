/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace datalight::protocol {

    template <typename T>
        struct pointerDerefCompare {
            bool operator()(const std::shared_ptr<T>& a, const std::shared_ptr<T>& b)
                const {
                return *a < *b;
            }
        };

    template <typename T>
        struct TupleDomain {
            std::shared_ptr<Map<T, Domain>> domains;
        };

    template <typename T>
        struct TupleDomain<std::shared_ptr<T>> {
        std::shared_ptr<std::map<std::shared_ptr<T>, Domain, pointerDerefCompare<T>>>
            domains;
    };

    template <class T>
        struct ColumnDomain {
            T column;
            Domain domain; // dependency
        };

}

namespace nlohmann {

    template <typename T>
        struct adl_serializer<datalight::protocol::ColumnDomain<T>> {
        static void to_json(
            json& j,
            const datalight::protocol::ColumnDomain<T>& p) {
            datalight::protocol::to_json_key(
                j, "column", p.column, "ColumnDomain", "T", "column");
            datalight::protocol::to_json_key(
                j, "domain", p.domain, "ColumnDomain", "Domain", "domain");
        }

        static void from_json(
            const json& j,
            datalight::protocol::ColumnDomain<T>& p) {
            datalight::protocol::from_json_key(
                j, "column", p.column, "ColumnDomain", "T", "column");
            datalight::protocol::from_json_key(
                j, "domain", p.domain, "ColumnDomain", "Domain", "domain");
        }
    };

    template <typename T>
        struct adl_serializer<datalight::protocol::TupleDomain<T>> {
        static void to_json(
            json& j,
            const datalight::protocol::TupleDomain<T>& tup) {
            datalight::protocol::List<
                datalight::protocol::ColumnDomain<T>>
                list;
            if (tup.domains != nullptr) {
                for (auto& el : *tup.domains) {
                    datalight::protocol::ColumnDomain<T> domain;
                    domain.column = el.first;
                    domain.domain = el.second;
                    list.push_back(domain);
                }
            }

            j["columnDomains"] = list;
        }

        static void from_json(
            const json& j,
            datalight::protocol::TupleDomain<T>& tup) {
            if (j.count("columnDomains") != 0U) {
                std::shared_ptr<datalight::protocol::
                    Map<T, datalight::protocol::Domain>>
                    map = std::make_shared<
                    std::map<T, datalight::protocol::Domain>>();

                datalight::protocol::List<
                    datalight::protocol::ColumnDomain<T>>
                    list = j.at("columnDomains");
                for (const datalight::protocol::ColumnDomain<T>& value : list) {
                    map->insert(std::make_pair(T(value.column), value.domain));
                }
                tup.domains = map;
            }
        }
    };

    template <typename T>
        struct adl_serializer<
        datalight::protocol::TupleDomain<std::shared_ptr<T>>> {
        static void to_json(
            json& j,
            const datalight::protocol::TupleDomain<std::shared_ptr<T>>& tup) {
            datalight::protocol::List<
                datalight::protocol::ColumnDomain<std::shared_ptr<T>>>
                list;
            if (tup.domains != nullptr) {
                for (auto& el : *tup.domains) {
                    datalight::protocol::ColumnDomain<std::shared_ptr<T>> domain;
                    domain.column = el.first;
                    domain.domain = el.second;
                    list.push_back(domain);
                }
            }

            j["columnDomains"] = list;
        }

        static void from_json(
            const json& j,
            datalight::protocol::TupleDomain<std::shared_ptr<T>>& tup) {
            if (j.count("columnDomains") != 0U) {
                auto map = std::make_shared<std::map<
                    std::shared_ptr<T>,
                    datalight::protocol::Domain,
                    datalight::protocol::pointerDerefCompare<T>>>();

                datalight::protocol::List<
                    datalight::protocol::ColumnDomain<std::shared_ptr<T>>>
                    list = j.at("columnDomains");
                for (const datalight::protocol::ColumnDomain<std::shared_ptr<T>>&
                         value : list) {
                    map->insert(
                        std::make_pair(std::shared_ptr<T>(value.column), value.domain));
                }
                tup.domains = map;
            }
        }
    };

}
