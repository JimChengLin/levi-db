#include <iostream>

#include "../src/env_io.h"
#include "../src/exception.h"

void env_io_test() {
    const std::string not_exist = "/tmp/JimZuoLin";
    if (LeviDB::IOEnv::fileExists(not_exist)) {
        LeviDB::IOEnv::deleteFile(not_exist);
    }

    try {
        LeviDB::IOEnv::getFileSize(not_exist);
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }

    try {
        LeviDB::IOEnv::deleteFile(not_exist);
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }

    try {
        LeviDB::IOEnv::renameFile(not_exist, not_exist + '_');
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }

    try {
        LeviDB::IOEnv::getChildren(not_exist);
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }
    assert(!LeviDB::IOEnv::getChildren("/tmp").empty());

    try {
        LeviDB::IOEnv::truncateFile(not_exist, 6);
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }

    try {
        LeviDB::IOEnv::deleteDir(not_exist);
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }

    try {
        (void) LeviDB::FileOpen(not_exist, LeviDB::IOEnv::R_M);
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }
    (void) LeviDB::FileOpen(not_exist, LeviDB::IOEnv::W_M);
    (void) LeviDB::FileOpen(not_exist, LeviDB::IOEnv::A_M);
    (void) LeviDB::FileOpen(not_exist, LeviDB::IOEnv::AP_M);
    LeviDB::IOEnv::deleteFile(not_exist);

    try {
        (void) LeviDB::FileFopen(not_exist, LeviDB::IOEnv::R_M);
        assert(false);
    } catch (const LeviDB::Exception & e) {
    }
    (void) LeviDB::FileFopen(not_exist, LeviDB::IOEnv::W_M);
    (void) LeviDB::FileFopen(not_exist, LeviDB::IOEnv::A_M);
    (void) LeviDB::FileFopen(not_exist, LeviDB::IOEnv::AP_M);
    (void) LeviDB::FileFopen(not_exist, LeviDB::IOEnv::RP_M);
    (void) LeviDB::FileFopen(not_exist, LeviDB::IOEnv::WP_M);
    LeviDB::IOEnv::deleteFile(not_exist);

    std::cout << __FUNCTION__ << std::endl;
}