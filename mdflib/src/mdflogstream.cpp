/*
 * Copyright 2022 Ingemar Hedvall
 * SPDX-License-Identifier: MIT
 */

#include "mdf/mdflogstream.h"

#include <iostream>
#include <array>
#include <string_view>

#include "mdf/mdfhelper.h"

namespace {

mdf::MdfLogFunction1 LogFunction1;
mdf::MdfLogFunction2 LogFunction2;

constexpr std::array<std::string_view, 9> kSeverityList = {"Trace", "Debug",
                                                       "Info", "Notice",
                                                       "Warning", "Error",
                                                       "Critical","Alert",
                                                       "Emergency" };
}  // end namespace

namespace mdf {

MdfLogStream::MdfLogStream(MdfLocation location, MdfLogSeverity severity)
    : location_(std::move(location)), severity_(severity) {}

MdfLogStream::~MdfLogStream() {
  MdfLogStream::LogString(location_, severity_, str());
}

void MdfLogStream::LogString(const MdfLocation &location, MdfLogSeverity severity,
                             const std::string &text) {
  if (LogFunction1) {
    LogFunction1(location, severity, text);
  }
  if (LogFunction2) {
    std::ostringstream func;
    func << location.file << ":" << location.function;
    LogFunction2(severity, func.str(), text);
  }
}

void MdfLogStream::SetLogFunction1(const MdfLogFunction1 &func) {
  LogFunction1 = func;
}

void MdfLogStream::SetLogFunction2(const MdfLogFunction2 &func) {
  LogFunction2 = func;
}

void MdfLogStream::ResetLogFunction() {
  LogFunction1 = nullptr;
  LogFunction2 = nullptr;
}

void MdfLogStream::LogToConsole(const MdfLocation& location,
                                   MdfLogSeverity severity,
                                   const std::string& text) {
  try {
    const uint64_t now = MdfHelper::NowNs();
    const std::string now_string = MdfHelper::NsToLocalIsoTime(now);
    std::cout << now_string << " ";
    if (const auto index = static_cast<size_t>(severity);
        index < kSeverityList.size()) {
      std::cout << "[" << kSeverityList[index] << "] ";
    }

    std::cout << text << " ";
    std::cout << "(" << location.file << ":"
                << location.function << " C:" << location.column
                << " L:" << location.line << ")";

    std::cout << std::endl;
  } catch (const std::exception&) {

  }
}


}  // namespace mdf