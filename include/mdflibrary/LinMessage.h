/*
 * Copyright 2026 Julles
 * SPDX-License-Identifier: MIT
 */
#pragma once
#include <stdexcept>
#include <vector>

#include "MdfExport.h"

using namespace MdfLibrary::ExportFunctions;

namespace MdfLibrary {
class LinMessage {
 private:
  bool isNew = false;
  mdf::LinMessage* lin;

 public:
  LinMessage(mdf::LinMessage* lin) : lin(lin) {
    if (lin == nullptr) throw std::runtime_error("LinMessage Init failed");
  }
  LinMessage() : LinMessage(LinMessageInit()) { isNew = true; };
  ~LinMessage() {
    if (isNew) LinMessageUnInit(lin);
    lin = nullptr;
  };
  LinMessage(const LinMessage&) = delete;
  LinMessage(LinMessage&& other) {
    if (isNew) LinMessageUnInit(this->lin);
    this->isNew = other.isNew;
    this->lin = other.lin;
    other.isNew = false;
    other.lin = nullptr;
  }

  mdf::LinMessage* GetLinMessage() const { return lin; }

  void SetLinId(uint8_t id) { LinMessageSetLinId(lin, id); };
  uint8_t GetLinId() const { return LinMessageGetLinId(lin); };

  void SetBusChannel(uint8_t channel) {
    LinMessageSetBusChannel(lin, channel);
  };
  uint8_t GetBusChannel() const { return LinMessageGetBusChannel(lin); };

  void SetDir(bool transmit) { LinMessageSetDir(lin, transmit); };
  bool GetDir() const { return LinMessageGetDir(lin); };

  void SetDataLength(uint8_t len) { LinMessageSetDataLength(lin, len); };
  uint8_t GetDataLength() const { return LinMessageGetDataLength(lin); };

  void SetReceivedDataByteCount(uint8_t len) {
    LinMessageSetReceivedDataByteCount(lin, len);
  };
  uint8_t GetReceivedDataByteCount() const {
    return LinMessageGetReceivedDataByteCount(lin);
  };

  std::vector<uint8_t> GetDataBytes() const {
    size_t count = LinMessageGetDataBytes(lin, nullptr);
    if (count <= 0) return {};
    auto data = std::vector<uint8_t>(count);
    LinMessageGetDataBytes(lin, data.data());
    return data;
  };
  void SetDataBytes(const std::vector<uint8_t>& data) {
    LinMessageSetDataBytes(lin, data.data(), data.size());
  };
  void SetDataBytes(const uint8_t* data, const size_t size) {
    LinMessageSetDataBytes(lin, data, size);
  };

  void SetChecksum(uint8_t crc) { LinMessageSetChecksum(lin, crc); };
  uint8_t GetChecksum() const { return LinMessageGetChecksum(lin); };

  void SetChecksumModel(int model) { LinMessageSetChecksumModel(lin, model); };
  int GetChecksumModel() const { return LinMessageGetChecksumModel(lin); };

  void SetStartOfFrame(uint64_t ns1970) {
    LinMessageSetStartOfFrame(lin, ns1970);
  };
  uint64_t GetStartOfFrame() const { return LinMessageGetStartOfFrame(lin); };

  void SetBaudrate(float baud) { LinMessageSetBaudrate(lin, baud); };
  float GetBaudrate() const { return LinMessageGetBaudrate(lin); };

  void SetResponseBaudrate(float baud) {
    LinMessageSetResponseBaudrate(lin, baud);
  };
  float GetResponseBaudrate() const {
    return LinMessageGetResponseBaudrate(lin);
  };

  void SetBreakLength(uint32_t len) { LinMessageSetBreakLength(lin, len); };
  uint32_t GetBreakLength() const { return LinMessageGetBreakLength(lin); };
};
}  // namespace MdfLibrary
