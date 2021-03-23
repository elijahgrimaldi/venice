package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.nio.ByteBuffer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * Utility functions for munging on bytes
 *
 * N.B.: Most functions taken from Voldemort's ByteUtils class.
 */
public class ByteUtils {

  public static final int BYTES_PER_KB = 1024;
  public static final int BYTES_PER_MB = BYTES_PER_KB * 1024;
  public static final long BYTES_PER_GB = BYTES_PER_MB * 1024;
  public static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;
  public static final int SIZE_OF_INT = Integer.SIZE / Byte.SIZE;
  public static final int SIZE_OF_SHORT = Short.SIZE / Byte.SIZE;
  public static final int SIZE_OF_BOOLEAN = 1;

  private final static int MAX_LENGTH_TO_LOG = 50;

  /**
   * Translate the given byte array into a hexadecimal string
   *
   * @param bytes The bytes to translate
   * @return The string
   */
  public static String toHexString(byte[] bytes) {
    return Hex.encodeHexString(bytes);
  }

  /**
   * Translate the given byte array with specific start position and length into a hexadecimal string
   */
  public static String toHexString(byte[] bytes, int start, int len) {
    byte[] newBytes = new byte[len];
    System.arraycopy(bytes, start, newBytes, 0, len);
    return Hex.encodeHexString(newBytes);
  }

  /**
   * Translate the given byte array to a String so that it can be used in logging.
   * This function handles truncation of the String to prevent log from overflowing.
   *
   * @param bytes
   * @return String
   */
  public static String toLogString(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }

    String str = toHexString(bytes);
    if (str.length() > MAX_LENGTH_TO_LOG) {
      return str.substring(0, MAX_LENGTH_TO_LOG) + "...truncated";
    }
    return str;
  }

  /**
   * Translate the given hexidecimal string into a byte array
   *
   * @param hexString The hex string to translate
   * @return The bytes
   * @throws DecoderException
   */
  public static byte[] fromHexString(String hexString) {
    try {
      return Hex.decodeHex(hexString.toCharArray());
    } catch (DecoderException e) {
      throw new VeniceException("Failed to convert from Hex to byte[]", e);
    }
  }

  /**
   * Write a long to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The long to write
   * @param offset The offset to begin writing at
   */
  public static void writeLong(byte[] bytes, long value, int offset) {
    bytes[offset] = (byte) (0xFF & (value >> 56));
    bytes[offset + 1] = (byte) (0xFF & (value >> 48));
    bytes[offset + 2] = (byte) (0xFF & (value >> 40));
    bytes[offset + 3] = (byte) (0xFF & (value >> 32));
    bytes[offset + 4] = (byte) (0xFF & (value >> 24));
    bytes[offset + 5] = (byte) (0xFF & (value >> 16));
    bytes[offset + 6] = (byte) (0xFF & (value >> 8));
    bytes[offset + 7] = (byte) (0xFF & value);
  }

  /**
   * Read a long from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The long read
   */
  public static long readLong(byte[] bytes, int offset) {
    return (((long) (bytes[offset + 0] & 0xff) << 56) | ((long) (bytes[offset + 1] & 0xff) << 48) | (
        (long) (bytes[offset + 2] & 0xff) << 40) | ((long) (bytes[offset + 3] & 0xff) << 32) | (
        (long) (bytes[offset + 4] & 0xff) << 24) | ((long) (bytes[offset + 5] & 0xff) << 16) | (
        (long) (bytes[offset + 6] & 0xff) << 8) | ((long) bytes[offset + 7] & 0xff));
  }

  /**
   * Write an int to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The int to write
   * @param offset The offset to begin writing at
   */
  public static void writeInt(byte[] bytes, int value, int offset) {
    bytes[offset] = (byte) (0xFF & (value >> 24));
    bytes[offset + 1] = (byte) (0xFF & (value >> 16));
    bytes[offset + 2] = (byte) (0xFF & (value >> 8));
    bytes[offset + 3] = (byte) (0xFF & value);
  }

  /**
   * Read an int from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The int read
   */
  public static int readInt(byte[] bytes, int offset) {
    return (((bytes[offset + 0] & 0xff) << 24) | ((bytes[offset + 1] & 0xff) << 16)
        | ((bytes[offset + 2] & 0xff) << 8) | (bytes[offset + 3] & 0xff));
  }

  /**
   * Write a short to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The short to write
   * @param offset The offset to begin writing at
   */
  public static void writeShort(byte[] bytes, short value, int offset) {
    bytes[offset] = (byte) (0xFF & (value >> 8));
    bytes[offset + 1] = (byte) (0xFF & value);
  }

  /**
   * Read a short from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The short read
   */
  public static short readShort(byte[] bytes, int offset) {
    return (short) ((bytes[offset] << 8) | (bytes[offset + 1] & 0xff));
  }

  /**
   * Write a boolean to the byte array starting at the given offset
   *
   * @param bytes  The byte array
   * @param value  The boolean to write
   * @param offset The offset to begin writing at
   */
  public static void writeBoolean(byte[] bytes, Boolean value, int offset) {
    bytes[offset] = (byte) (value ? 0x01 : 0x00);
  }

  /**
   * Read a boolean from the byte array starting at the given offset
   *
   * @param bytes  The byte array to read from
   * @param offset The offset to start reading at
   * @return The boolean read
   */
  public static boolean readBoolean(byte[] bytes, int offset) {
    return bytes[offset] == 0x01;
  }

  /**
   * A comparator for byte arrays.
   *
   * Taken from: https://stackoverflow.com/a/5108711/791758 (and originally coming for Apache HBase)
   */
  public static int compare(byte[] left, byte[] right) {
    for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
      int a = (left[i] & 0xff);
      int b = (right[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }

  /**
   * Compare whether two byte array is the same from specific offsets.
   */
  public static boolean equals(byte[] left, int leftPosition, byte[] right, int rightPosition) {
    if (left.length - leftPosition != right.length - rightPosition) {
      return false;
    }
    for (int i = leftPosition, j = 0; i < left.length; i++, j++) {
      if (left[i] != right[rightPosition + j]) {
        return false;
      }
    }
    return true;
  }

  public static boolean canUseBackedArray(ByteBuffer byteBuffer) {
    return byteBuffer.array().length == byteBuffer.remaining();
  }

  public static byte[] extractByteArray(ByteBuffer byteBuffer) {
    if (ByteUtils.canUseBackedArray(byteBuffer)) {
      // We could safely use the backed array.
      return byteBuffer.array();
    }
    byte[] value = new byte[byteBuffer.remaining()];
    System.arraycopy(byteBuffer.array(), byteBuffer.position(), value, 0, byteBuffer.remaining());
    return value;
  }
}