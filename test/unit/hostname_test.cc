#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <string>

// GUC stubs — only the variables referenced by otel_exporter.cc are needed here.
// The full definitions live in src/config/guc.cc (requires the PostgreSQL server).
char* psch_hostname = nullptr;
char* psch_otel_endpoint = nullptr;
int psch_otel_log_queue_size = 4096;
int psch_otel_log_batch_size = 512;
int psch_otel_log_max_bytes = 4194304;
int psch_otel_log_delay_ms = 200;
int psch_otel_metric_interval_ms = 5000;

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
  static char guc_host[] = "guc-host";
  psch_hostname = guc_host;
  setenv("HOSTNAME", "env-host", /*overwrite=*/1);
  EXPECT_EQ(GetAHostname("fallback"), "guc-host");
}

TEST_F(GetAHostnameTest, EmptyGucFallsToEnv) {
  static char empty_guc[] = "";
  psch_hostname = empty_guc;
  setenv("HOSTNAME", "env-host", 1);
  EXPECT_EQ(GetAHostname("fallback"), "env-host");
}

TEST_F(GetAHostnameTest, NullGucFallsToEnv) {
  setenv("HOSTNAME", "env-host", 1);
  EXPECT_EQ(GetAHostname("fallback"), "env-host");
}

// The fallback string is only returned if gethostname() itself fails, which
// cannot be forced in a unit test without intercepting the syscall.

TEST_F(GetAHostnameTest, ResultIsNeverEmpty) {
  EXPECT_FALSE(GetAHostname("fallback").empty());
}

}  // namespace
