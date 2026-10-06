if (!`SELECT COUNT(*) FROM information_schema.ENGINES WHERE ENGINE = 'MooseDB' AND SUPPORT IN ('YES', 'DEFAULT')`)
{
  --skip Test requires the MooseDB storage engine (ha_moosedb.so)
}
