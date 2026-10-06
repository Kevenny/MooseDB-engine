if (!`SELECT COUNT(*) FROM information_schema.ENGINES WHERE ENGINE = 'TideFlow' AND SUPPORT IN ('YES', 'DEFAULT')`)
{
  --skip Test requires the TideFlow storage engine (ha_tideflow.so)
}
