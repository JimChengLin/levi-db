#include <algorithm>

#include "../db_single.h"
#include "aggregator.h"
#include "compact_1_2.h"

namespace LeviDB {
    Aggregator::Aggregator(std::string name, Options options)
            : DB(std::move(name), options) {
        std::string prefix = _name + '/';

        if (IOEnv::fileExists(_name)) {
            if (_options.error_if_exists) {
                throw Exception::invalidArgumentException("DB already exists");
            }
            // 打开现有数据库
            _file_lock.build(prefix + "lock");
            _meta.build(prefix + "keeper");

            // 确认兼容
            if (_meta->immut_value().format_version > AggregatorStrongMeta{}.format_version ||
                _meta->immut_value().db_version > AggregatorStrongMeta{}.db_version) {
                throw Exception::invalidArgumentException("target's format is not supported");
            }

            // 获取分片信息并简单修复
            std::vector<std::string> children = IOEnv::getChildren(prefix);
            for (const std::string & child:children) {
                std::string prefixed_child;
                if (not(child[0] >= '0' && child[0] <= '9')
                    || (prefixed_child = prefix + child, !IOEnv::fileExists(prefixed_child))) {
                    continue;
                }

                decltype(child.find(char{})) pos{};
                if ((pos = child.find('+')) != std::string::npos) { // compact 2 to 1
                    if (child.back() != '-') { // fail, remove product
                        for (const std::string & c:LeviDB::IOEnv::getChildren(prefixed_child)) {
                            LeviDB::IOEnv::deleteFile((prefixed_child + '/') += c);
                        }
                        LeviDB::IOEnv::deleteDir(prefixed_child);
                    } else { // success, remove resources
                        for (const std::string & n:{std::string(prefix).append(child.cbegin(), child.cbegin() + pos),
                                                    std::string(prefix).append(
                                                            child.cbegin() + (pos + 1), --child.cend())}) {
                            for (const std::string & c:LeviDB::IOEnv::getChildren(n)) {
                                LeviDB::IOEnv::deleteFile((n + '/') += c);
                            }
                            LeviDB::IOEnv::deleteDir(n);
                        }
                    }
                } else if ((pos = child.find('_')) != std::string::npos) { // compact 1 to 2
                    prefixed_child.resize(prefixed_child.size() - (child.size() - pos));
                    repairCompacting1To2DB(prefixed_child, [](const Exception & e) { throw e; });
                }
            }

            children = IOEnv::getChildren(prefix);
            children.erase(std::remove_if(children.begin(), children.end(), [&prefix](std::string & child) noexcept {
                child = prefix + child;
                return not(child.back() >= '0' && child.back() <= '9');
            }), children.end());
            // 写入 search_map
            for (std::string & child:children) {
                WeakKeeper<DBSingleWeakMeta> m(child + "/keeper");
                const std::string & trailing = m.immut_trailing();
                uint32_t from_k_len = m.immut_value().from_k_len;

                auto node = std::make_unique<AggregatorNode>();
                node->lower_bound = std::string(m.immut_trailing().data(),
                                                m.immut_trailing().data() + from_k_len);
                node->db_name = std::move(child);
                insertNode(std::move(node));
            }
        } else {
            if (!_options.create_if_missing) {
                throw Exception::notFoundException("DB not found");
            }
            // 新建数据库
            IOEnv::createDir(_name);
            _file_lock.build(prefix + "lock");
            _meta.build(prefix + "keeper", AggregatorStrongMeta{}, std::string{});

            // 至少存在一个数据库分片
            auto node = std::make_unique<AggregatorNode>();
            Options opt;
            opt.create_if_missing = true;
            opt.error_if_exists = true;
            node->db = std::make_unique<DBSingle>(std::to_string(_meta->immut_value().counter), opt, &_seq_gen);
            _meta->update(offsetof(AggregatorStrongMeta, counter), _meta->immut_value().counter + 1);
            node->db_name = node->db->immut_name();
        }

        // 日志
        std::string logger_prev_fname = prefix + "log_prev.txt";
        if (IOEnv::fileExists(logger_prev_fname)) {
            IOEnv::deleteFile(logger_prev_fname);
        }
        std::string logger_fname = std::move(prefix) + "log.txt";
        if (IOEnv::fileExists(logger_fname)) {
            IOEnv::renameFile(logger_fname, logger_prev_fname);
        }
        _logger.build(logger_fname);
        Logger::logForMan(_logger.get(), "start");
    };

    Aggregator::~Aggregator() noexcept {
        Logger::logForMan(_logger.get(), "end");
    }

    bool Aggregator::put(const WriteOptions & options,
                         const Slice & key,
                         const Slice & value) {

    };

    bool Aggregator::remove(const WriteOptions & options,
                            const Slice & key) {

    };

    bool Aggregator::write(const WriteOptions & options,
                           const std::vector<std::pair<Slice, Slice>> & kvs) {

    };

    std::pair<std::string, bool>
    Aggregator::get(const ReadOptions & options, const Slice & key) const {

    };

    std::unique_ptr<Snapshot>
    Aggregator::makeSnapshot() {

    };

    std::unique_ptr<Iterator<Slice, std::string>>
    Aggregator::makeIterator(std::unique_ptr<Snapshot> && snapshot) const {

    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexIterator(std::shared_ptr<Regex::R> regex,
                                  std::unique_ptr<Snapshot> && snapshot) const {

    };

    std::unique_ptr<SimpleIterator<std::pair<Slice, std::string>>>
    Aggregator::makeRegexReversedIterator(std::shared_ptr<Regex::R> regex,
                                          std::unique_ptr<Snapshot> && snapshot) const {

    };

    void Aggregator::tryApplyPending() {

    };

    bool Aggregator::canRelease() const {

    };

    Slice Aggregator::largestKey() const {

    };

    Slice Aggregator::smallestKey() const {

    };

    void Aggregator::updateKeyRange() {

    };

    bool Aggregator::explicitRemove(const WriteOptions & options,
                                    const Slice & key) {

    };

    void Aggregator::sync() {

    };
}