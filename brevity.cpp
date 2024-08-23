#include "brevity.h"
#include "print"
#include <string>

static unsigned char msb(uint64_t value) {
  unsigned char count = 0;
  while (value != 0) {
    ++count;
    value >>= 1;
  }
  return count;
}

static unsigned char byteCount(uint64_t value) {
  if (value == 0)
    return 1;
  return (msb(value) + 7) / 8;
}

void brevity_writer::write_varint_with_metabit(bool meta, uint64_t value) {
  _ensure_enough_for_varint();
  return write_varint_with_metabit_internal(meta, value);
}

void brevity_writer::write_varint_with_metabit_internal(bool meta, uint64_t value) {
  bool canFitIn5Bytes =
    value < (1ull << ((4 * 8) + 2)); // we have 4 bits for size, 1 for 0, 1 for meta, 2 for value
  if (canFitIn5Bytes) {
    value = value << 1;
    if (meta) {
      value |= 1;
    }
    value = value << 1; // add 0
    unsigned char bCount = 1;
    // we know that we are at least 1 byte
    // lets find out how many bytes we are going to need to write
    // each time we add a 1 to the least significant bit
    // when we finally have the same byte count as the value fits in
    // we should have 0, [1, 1, ...1] Where the count of 1's are final byte count -1
    while (bCount < byteCount(value)) {
      value = (value << 1) | 1; // add add first 1 for first byte
      ++bCount;
    }
    // we can always do a 8 byte store
    memcpy(_cur, &value, 8);
    _cur += bCount;
    return;
  }

  unsigned char bCount = byteCount(value);
  unsigned char fByte = (bCount - 5) << 5; // set 2&3rd MSBs
  fByte |= 0b11111;                        // set lowest 5 bits
  if (meta) {
    fByte |= 0b10000000; // set high bit
  }

  memcpy(_cur, &fByte, 1);
  _cur += 1;
  // we have already ensured that we have 9 bytes here
  //always do 8 copy so its faster
  memcpy(_cur, &value, 8);
  _cur += bCount;
}

void brevity_writer::write_varint(uint64_t value) {
  write_varint_with_metabit(value & 1, value >> 1);
}

uint64_t sign_pack(int64_t value) {
  return (value << 1) ^ (value >> (8 * sizeof(value) - 1));
}

int64_t sign_unpack(uint64_t v) {
  return static_cast<int64_t>((v >> 1) ^ static_cast<uint64_t>(-static_cast<int64_t>(v & 1)));
}

void brevity_writer::write_varint_signed(int64_t value) {
  write_varint(sign_pack(value));
}

[[noreturn]]
void brevity_reader::throw_parse_error_v(std::string_view message, std::format_args args) {
  throw std::runtime_error(std::vformat(message, args));
}

unsigned char GetFirstZeroShiftCount(unsigned char candidate) {
  unsigned char count = 0;
  while ((candidate & 1) != 0) {
    candidate >>= 1;
    ++count;
  }
  return count;
}

uint64_t brevity_reader::accumulate(unsigned char count) {
  uint64_t n = 0;
  if (buf.size() < count)
    throw_parse_error("at end");

  if (count == 0)
    return 0;

  if (count > sizeof(n))
    throw_parse_error("Trying to Accumulate too many bytes");

  memcpy(&n, buf.data(), count);

  buf = buf.subspan(count);
  return n;
}

std::pair<uint64_t, bool> brevity_reader::read_varint_n_check_len_every_byte() {
  if (buf.empty())
    throw_parse_error("at end");

  unsigned char candidate = buf.front();

  // get bytes to read
  auto shiftCount = GetFirstZeroShiftCount(candidate);

  if (shiftCount > 4) {
    buf = buf.subspan(1);
    // here we use the 2nd &3 most significant bits to represent how many
    // bytes to read more than 5
    unsigned char twoAndThreeMSB = candidate & 0b01100000;
    unsigned char twoAndThreeMSBAsNumber = twoAndThreeMSB >> 5;
    //determine the number of bytes to take additionally from the stream
    uint64_t toReturn = accumulate(5 + twoAndThreeMSBAsNumber);
    return {toReturn, candidate & 0b10000000};
  }

  uint64_t toReturn = accumulate(shiftCount + 1);
  toReturn >>= (1 + shiftCount);
  // strip off the metadata bit
  return {toReturn >> 1, toReturn & 0b1};
}

uint64_t brevity_reader::read_varint() {
  auto val = read_varint_n_check_len_every_byte();
  uint64_t toReturn = val.first << 1;
  if (val.second) {
    toReturn |= 1;
  }
  return toReturn;
}

std::pair<uint64_t, bool> brevity_reader::read_varint_with_metabit() {
  return read_varint_n_check_len_every_byte();
}

int64_t brevity_reader::read_varint_signed() {
  return sign_unpack(read_varint());
}

std::optional<uint64_t> brevity_reader::try_read_varint() {
  if (buf.empty())
    return std::nullopt;

    return read_varint();
}

std::optional<int64_t> brevity_reader::try_read_varint_signed() {
  if (buf.empty())
    return std::nullopt;
  return sign_unpack(read_varint());
}

