#include "query5.hpp"
#include<unordered_set>
#include<unordered_map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[],
               std::string& r_name,
               std::string& start_date,
               std::string& end_date,
               int& num_threads,
               std::string& table_path,
               std::string& result_path) {

    // Default values (optional safety)
    num_threads = 1;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        }
        else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        }
        else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        }
        else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        }
        else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        }
        else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        }
        else {
            std::cerr << "Unknown or incomplete argument: " << arg << std::endl;
            return false;
        }
    }

    // Validate required arguments
    if (r_name.empty() || start_date.empty() || end_date.empty() ||
        table_path.empty() || result_path.empty()) {

        std::cerr << "Missing required arguments\n";
        std::cerr << "Usage:\n";
        std::cerr << "./tpch_query5 "
                  << "--r_name ASIA "
                  << "--start_date 1994-01-01 "
                  << "--end_date 1995-01-01 "
                  << "--threads 4 "
                  << "--table_path /path/to/tables "
                  << "--result_path output.txt\n";

        return false;
    }

    // Validate thread count
    if (num_threads <= 0) {
        std::cerr << "Invalid thread count. Must be > 0\n";
        return false;
    }

    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(
    const std::string& table_path,
    std::vector<std::map<std::string, std::string>>& customer_data,
    std::vector<std::map<std::string, std::string>>& orders_data,
    std::vector<std::map<std::string, std::string>>& lineitem_data,
    std::vector<std::map<std::string, std::string>>& supplier_data,
    std::vector<std::map<std::string, std::string>>& nation_data,
    std::vector<std::map<std::string, std::string>>& region_data) {

    // Helper lambda to read a table
    auto readTable = [](const std::string& file,
                        std::vector<std::map<std::string, std::string>>& data,
                        const std::vector<std::string>& columns) -> bool {

        std::ifstream in(file);
        if (!in.is_open()) {
            std::cerr << "Error opening file: " << file << std::endl;
            return false;
        }

        std::string line;
        while (getline(in, line)) {
            std::stringstream ss(line);
            std::string token;
            std::map<std::string, std::string> row;

            int i = 0;
            while (getline(ss, token, '|')) {
                if (i < columns.size()) {
                    row[columns[i]] = token;
                }
                i++;
            }

            data.push_back(row);
        }

        return true;
    };

    // Read only required columns for Query 5

    if (!readTable(table_path + "/customer.tbl", customer_data,
                   {"c_custkey", "c_name", "c_address", "c_nationkey"}))
        return false;

    if (!readTable(table_path + "/orders.tbl", orders_data,
                   {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"}))
        return false;

    if (!readTable(table_path + "/lineitem.tbl", lineitem_data,
                   {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
                    "l_quantity", "l_extendedprice", "l_discount"}))
        return false;

    if (!readTable(table_path + "/supplier.tbl", supplier_data,
                   {"s_suppkey", "s_name", "s_address", "s_nationkey"}))
        return false;

    if (!readTable(table_path + "/nation.tbl", nation_data,
                   {"n_nationkey", "n_name", "n_regionkey"}))
        return false;

    if (!readTable(table_path + "/region.tbl", region_data,
                   {"r_regionkey", "r_name"}))
        return false;

    return true;
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(
    const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>>& customer_data,
    const std::vector<std::map<std::string, std::string>>& orders_data,
    const std::vector<std::map<std::string, std::string>>& lineitem_data,
    const std::vector<std::map<std::string, std::string>>& supplier_data,
    const std::vector<std::map<std::string, std::string>>& nation_data,
    const std::vector<std::map<std::string, std::string>>& region_data,
    std::map<std::string, double>& results) {

    // ------------------ STEP 1: Build lookup tables ------------------

    std::unordered_map<std::string, std::string> cust_to_nation;
    for (const auto& c : customer_data)
        cust_to_nation[c.at("c_custkey")] = c.at("c_nationkey");

    std::unordered_map<std::string, std::string> supp_to_nation;
    for (const auto& s : supplier_data)
        supp_to_nation[s.at("s_suppkey")] = s.at("s_nationkey");

    std::unordered_map<std::string, std::string> nation_to_region;
    std::unordered_map<std::string, std::string> nation_name;
    for (const auto& n : nation_data) {
        nation_to_region[n.at("n_nationkey")] = n.at("n_regionkey");
        nation_name[n.at("n_nationkey")] = n.at("n_name");
    }

    std::string target_region_key;
    for (const auto& r : region_data) {
        if (r.at("r_name") == r_name) {
            target_region_key = r.at("r_regionkey");
            break;
        }
    }

    // Filter valid nations in region
    std::unordered_set<std::string> valid_nations;
    for (const auto& [nationkey, regionkey] : nation_to_region) {
        if (regionkey == target_region_key)
            valid_nations.insert(nationkey);
    }

    // Filter orders by date
    std::unordered_map<std::string, std::string> order_to_cust;
    for (const auto& o : orders_data) {
        std::string date = o.at("o_orderdate");
        if (date >= start_date && date < end_date) {
            order_to_cust[o.at("o_orderkey")] = o.at("o_custkey");
        }
    }

    // ------------------ STEP 2: Multithreading ------------------

    std::mutex mtx;
    std::vector<std::thread> threads;

    int total = lineitem_data.size();
    int chunk = total / num_threads;

    auto worker = [&](int start, int end) {
        std::unordered_map<std::string, double> local_result;

        for (int i = start; i < end; i++) {
            const auto& l = lineitem_data[i];

            std::string orderkey = l.at("l_orderkey");
            std::string suppkey  = l.at("l_suppkey");

            if (order_to_cust.find(orderkey) == order_to_cust.end())
                continue;

            std::string custkey = order_to_cust.at(orderkey);

            std::string cust_nat = cust_to_nation.at(custkey);
            std::string supp_nat = supp_to_nation.at(suppkey);

            // Join condition: same nation
            if (cust_nat != supp_nat)
                continue;

            // Region filter
            if (valid_nations.find(cust_nat) == valid_nations.end())
                continue;

            double price = std::stod(l.at("l_extendedprice"));
            double discount = std::stod(l.at("l_discount"));

            double revenue = price * (1.0 - discount);

            std::string nation = nation_name.at(cust_nat);

            local_result[nation] += revenue;
        }

        // Merge results safely
        std::lock_guard<std::mutex> lock(mtx);
        for (const auto& [nation, rev] : local_result) {
            results[nation] += rev;
        }
    };

    // Launch threads
    for (int i = 0; i < num_threads; i++) {
        int start = i * chunk;
        int end = (i == num_threads - 1) ? total : start + chunk;

        threads.emplace_back(worker, start, end);
    }

    // Join threads
    for (auto& t : threads) t.join();

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path,
                   const std::map<std::string, double>& results) {

    std::ofstream out(result_path);
    if (!out.is_open()) {
        std::cerr << "Error: Unable to open output file: " << result_path << std::endl;
        return false;
    }

    // Convert map → vector for sorting
    std::vector<std::pair<std::string, double>> vec(results.begin(), results.end());

    // Sort by revenue DESC
    std::sort(vec.begin(), vec.end(),
              [](const auto& a, const auto& b) {
                  return a.second > b.second;
              });

    // Write output
    for (const auto& [nation, revenue] : vec) {
        out << nation << " | " << revenue << "\n";
    }

    out.close();
    return true;
}
