/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** kvstore_enc.h — Private encryption helpers for SNKV.
**
** Do NOT include this from public headers.  Only kvstore.c includes it.
**
** Crypto stack:
**   KDF  : Argon2id (via Monocypher) — derives 256-bit key from password+salt
**   AEAD : XChaCha20-Poly1305 (via Monocypher) — encrypts each value
**   Wipe : crypto_wipe — zeroes sensitive memory on close/rekey
**
** Encrypted value layout (stored in the B-tree):
**   [nonce(24)][ciphertext(N)][mac(16)]   total overhead = 40 bytes
*/

#ifndef KVSTORE_ENC_H
#define KVSTORE_ENC_H

#include <stdint.h>
#include <stddef.h>

/* -------------------------------------------------------------------------
** Argon2id KDF parameters stored in the auth CF
** (m_cost in kibibytes, t_cost = passes, lanes always 1)
** ------------------------------------------------------------------------- */
#define SNKV_ENC_SALT_LEN     16   /* salt bytes stored in auth CF */
#define SNKV_ENC_KEY_LEN      32   /* derived key length            */
#define SNKV_ENC_NONCE_LEN    24   /* XChaCha20 nonce length        */
#define SNKV_ENC_MAC_LEN      16   /* Poly1305 tag length           */
#define SNKV_ENC_OVERHEAD     40   /* NONCE_LEN + MAC_LEN           */

/* Default Argon2id cost (tunable at open time via future config) */
#define SNKV_ENC_M_COST   65536u   /* 64 MiB */
#define SNKV_ENC_T_COST       3u   /* 3 passes */

/* Auth CF name and magic verify token */
#define SNKV_AUTH_CF_NAME  "__snkv_auth__"
#define SNKV_AUTH_VERIFY   "SNKV_AUTH_V1"  /* 12 bytes, encrypted as verify tag */
#define SNKV_AUTH_VERIFY_LEN 12

/* Keys inside the auth CF */
#define SNKV_AUTH_KEY_SALT    "salt"   /* 16-byte random salt        */
#define SNKV_AUTH_KEY_PARAMS  "params" /* 8-byte [m_cost(4)][t_cost(4)] big-endian */
#define SNKV_AUTH_KEY_VERIFY  "verify" /* encrypted SNKV_AUTH_VERIFY */

/* -------------------------------------------------------------------------
** kvstoreEncRandBytes — fill buf with len cryptographically random bytes.
** Returns 0 on success, -1 on failure.
** ------------------------------------------------------------------------- */
int kvstoreEncRandBytes(uint8_t *buf, size_t len);

/* -------------------------------------------------------------------------
** kvstoreEncDeriveKey — run Argon2id and store the 32-byte result in aKey.
**   pPassword / nPassword — raw password
**   aSalt                 — SNKV_ENC_SALT_LEN bytes of salt
**   mCost                 — memory kibibytes
**   tCost                 — passes
** Returns 0 on success, -1 on malloc failure.
** ------------------------------------------------------------------------- */
int kvstoreEncDeriveKey(
  uint8_t        aKey[SNKV_ENC_KEY_LEN],
  const void    *pPassword, int nPassword,
  const uint8_t  aSalt[SNKV_ENC_SALT_LEN],
  uint32_t       mCost,
  uint32_t       tCost
);

/* -------------------------------------------------------------------------
** kvstoreEncEncrypt — encrypt pPlain[nPlain] → pOut.
**   pOut must hold at least nPlain + SNKV_ENC_OVERHEAD bytes.
**   aKey must be SNKV_ENC_KEY_LEN bytes.
**   A fresh random nonce is generated internally.
** Layout written to pOut: [nonce(24)][ciphertext(nPlain)][mac(16)]
** Returns 0 on success, -1 on RNG failure.
** ------------------------------------------------------------------------- */
int kvstoreEncEncrypt(
  uint8_t       *pOut,
  const uint8_t  aKey[SNKV_ENC_KEY_LEN],
  const uint8_t *pPlain, int nPlain
);

/* -------------------------------------------------------------------------
** kvstoreEncDecrypt — decrypt pEnc[nEnc] → pOut.
**   nEnc must be >= SNKV_ENC_OVERHEAD; plaintext length = nEnc - SNKV_ENC_OVERHEAD.
**   pOut must hold at least nEnc - SNKV_ENC_OVERHEAD bytes.
**   aKey must be SNKV_ENC_KEY_LEN bytes.
** Returns 0 on success, -1 on MAC mismatch (tampered/wrong key).
** ------------------------------------------------------------------------- */
int kvstoreEncDecrypt(
  uint8_t       *pOut,
  const uint8_t  aKey[SNKV_ENC_KEY_LEN],
  const uint8_t *pEnc, int nEnc
);

/* -------------------------------------------------------------------------
** kvstoreEncWipe — securely zero sensitive memory.
** ------------------------------------------------------------------------- */
void kvstoreEncWipe(void *p, size_t n);

#endif /* KVSTORE_ENC_H */
