import zstandard as zstd

pastelid = "jXXdC3owuCUKFwUjVPxhudPdc3sB9q2rm4Ez7zxRMPFciunY6QoK3G3UpGCz16odv6gZwByWsazVFkN6CpSvsS"
signature = "3ug2tRROldwKPRnvdJohiaP3TS3wwtyka9xzYjMX8jxOlXL7DTfvyF6AmvVYCdybS1TSXmjEv6KAvdiSnZ806B8u+1kZbSBXfCrdrhupbeoDFPCMKHre6HOlEhPSeh8HPQHxqqX91u7JD8qX7ejEfyYA"
concatenated_data = pastelid + '|' + signature

# Calculate the byte size
uncompressed_byte_size = len(concatenated_data.encode('utf-8'))
print(f"Uncompressed byte size: {uncompressed_byte_size}")

# Compress the concatenated string using z-standard with level 22 compression
compressor = zstd.ZstdCompressor(level=22)
compressed_data_zstd = compressor.compress(concatenated_data.encode('utf-8'))
compressed_size_zstd = len(compressed_data_zstd)

print(f"Compressed byte size (z-standard level 22): {compressed_size_zstd}")
