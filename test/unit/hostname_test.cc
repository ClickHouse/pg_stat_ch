#include <gtest/gtest.h>

#include <string>

// GUC stubs (defined in guc.cc in the extension; not linked here)
char* psch_hostname = nullptr;
char* psch_otel_endpoint = nullptr;

// Stub out the PG-dependent log helper declared in exporter_interface.h
void LogNegativeValue(const std::string&, int64_t) {}

// Function under test — defined in otel_exporter.cc with external linkage
std::string GetAHostname(const char* fallback);

namespace {

class GetAHostnameTest : public ::testing::Test {
 protected:
  void SetUp() override {
    psch_hostname = nullptr;
    unsetenv("HOSTNAME");
  }
  void TearDown() override {
    psch_hostname = nullptr;
    unsetenv("HOSTNAME");
  }
};

TEST_F(GetAHostnameTest, GucTakesPrecedence) {
  psch_hostname = const_cast<char*>("guc-host");
  setenv("HOSTNAME", "env-host", /*overwrite=*/1);
  EXPECT_EQ(GetAHostname("fallback"), "guc-host");
}

TEST_F(GetAHostnameTest, EmptyGucFallsToEnv) {
  psch_hostname = const_cast<char*>("");
  setenv("HOSTNAME", "env-host", 1);
  EXPECT_EQ(GetAHostname("fallback"), "env-host");
}

TEST_F(GetAHostnameTest, NullGucFallsToEnv) {
  setenv("HOSTNAME", "env-host", 1);
  EXPECT_EQ(GetAHostname("fallback"), "env-host");
}

TEST_F(GetAHostnameTest, FallsToGethostnameWhenEnvAbsent) {
  EXPECT_NE(GetAHostname("fallback"), "fallback");
}

TEST_F(GetAHostnameTest, ResultIsNeverEmpty) {
  EXPECT_FALSE(GetAHostname("fallback").empty());
}

}  // namespace
