package metadata

remap: functions: encrypt: {
	category: "Cryptography"
	description: """
		Encrypts a string with a symmetric encryption algorithm.

		Supported Algorithms:

		* AES-256-CFB (key = 32 bytes, iv = 16 bytes)
		* AES-192-CFB (key = 24 bytes, iv = 16 bytes)
		* AES-128-CFB (key = 16 bytes, iv = 16 bytes)
		* AES-256-OFB (key = 32 bytes, iv = 16 bytes)
		* AES-192-OFB  (key = 24 bytes, iv = 16 bytes)
		* AES-128-OFB (key = 16 bytes, iv = 16 bytes)
		* AES-256-CTR (key = 32 bytes, iv = 16 bytes)
		* AES-192-CTR (key = 24 bytes, iv = 16 bytes)
		* AES-128-CTR (key = 16 bytes, iv = 16 bytes)
		* AES-256-CBC-PKCS7 (key = 32 bytes, iv = 16 bytes)
		* AES-192-CBC-PKCS7 (key = 24 bytes, iv = 16 bytes)
		* AES-128-CBC-PKCS7 (key = 16 bytes, iv = 16 bytes)
		* AES-256-CBC-ANSIX923 (key = 32 bytes, iv = 16 bytes)
		* AES-192-CBC-ANSIX923 (key = 24 bytes, iv = 16 bytes)
		* AES-128-CBC-ANSIX923 (key = 16 bytes, iv = 16 bytes)
		* AES-256-CBC-ISO7816 (key = 32 bytes, iv = 16 bytes)
		* AES-192-CBC-ISO7816 (key = 24 bytes, iv = 16 bytes)
		* AES-128-CBC-ISO7816 (key = 16 bytes, iv = 16 bytes)
		* AES-256-CBC-ISO10126 (key = 32 bytes, iv = 16 bytes)
		* AES-192-CBC-ISO10126 (key = 24 bytes, iv = 16 bytes)
		* AES-128-CBC-ISO10126 (key = 16 bytes, iv = 16 bytes)
		"""

	arguments: [
		{
			name:        "plaintext"
			description: "The string to encrypt."
			required:    true
			type: ["string"]
		},
		{
			name:        "algorithm"
			description: "The algorithm to use."
			required:    true
			type: ["string"]
		},
		{
			name:        "key"
			description: "The key for encryption. The should be raw bytes of the key (not encoded). The length must match the algorithm requested."
			required:    true
			type: ["string"]
		},
		{
			name: "iv"
			description: #"""
				The IV for encryption. The should be raw bytes of the IV (not encoded). The length must match the algorithm requested.
				A new IV should be generated for every message. You can use `random_bytes` to generate a cryptographically secure random value.
				"""#
			required: true
			type: ["string"]
		},
	]
	internal_failure_reasons: [
		"`algorithm` isn't a supported algorithm",
		"`key` length doesn't match the key size required for the algorithm specified",
		"`iv` length doesn't match the iv size required for the algorithm specified",
	]
	return: types: ["string"]

	examples: [
		{
			title: "Encrypt value"
			source: #"""
				plaintext = "super secret message"
				iv = "1234567890123456" # typically you would call random_bytes(16)
				key = "16_byte_keyxxxxx"
				encrypted_message = encrypt!(plaintext, "AES-128-CBC-PKCS7", key, iv: iv)
				encode_base64(encrypted_message)
				"""#
			return: "GBw8Mu00v0Kc38+/PvsVtGgWuUJ+ZNLgF8Opy8ohIYE="
		},
	]
}
