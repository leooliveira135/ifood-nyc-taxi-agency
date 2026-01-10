output "credentials" {
  value = {
    for k, v in local.users : k => {
      "key"      = aws_iam_access_key.user_access_key[k].id
      "secret"   = aws_iam_access_key.user_access_key[k].secret
      "password" = data.pgp_decrypt.user_password_decrypt[k].plaintext
    }
  }
  sensitive = true
}