#
# Helper for statement atomicity tests. The caller has already --send'ed a
# long statement on another connection and set:
#   $poll_table   table to read
#   $poll_info    LIKE pattern of the running statement in PROCESSLIST
#   $poll_n1      first allowed row count besides 0 (the final count)
#   $poll_n2      second allowed count (set equal to $poll_n1 if only one)
# Reads COUNT(*) repeatedly while the statement runs and sets:
#   $partial_seen  number of reads that returned a count not in {0, n1, n2}
#   $during        number of reads taken while the statement was running
#
--let $wait_condition= SELECT COUNT(*) >= 1 FROM information_schema.PROCESSLIST WHERE INFO LIKE '$poll_info' AND ID != CONNECTION_ID()
--source include/wait_condition.inc
--let $partial_seen= 0
--let $during= 0
--let $iter= 0
--let $running= 1
while ($running)
{
  --let $before= `SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE '$poll_info' AND ID != CONNECTION_ID()`
  --let $c= `SELECT COUNT(*) FROM $poll_table`
  --let $running= `SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE '$poll_info' AND ID != CONNECTION_ID()`
  if ($before)
  {
    if ($running)
    {
      --inc $during
    }
  }
  if ($c != 0)
  {
    if ($c != $poll_n1)
    {
      if ($c != $poll_n2)
      {
        --inc $partial_seen
      }
    }
  }
  --inc $iter
  if ($iter > 3000)
  {
    --let $running= 0
  }
}
--echo partial_seen = $partial_seen
--let $observed= `SELECT $during > 0`
--echo observed_during_insert = $observed
