using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window
{
	public class TestRollingWindowManager
	{
		internal Configuration conf;

		internal RollingWindowManager manager;

		internal string[] users;

		internal const int Min2Ms = 60000;

		internal readonly int WindowLenMs = 1 * Min2Ms;

		internal readonly int BucketCnt = 10;

		internal readonly int NTopUsers = 10;

		internal readonly int BucketLen = WindowLenMs / BucketCnt;

		[SetUp]
		public virtual void Init()
		{
			conf = new Configuration();
			conf.SetInt(DFSConfigKeys.NntopBucketsPerWindowKey, BucketCnt);
			conf.SetInt(DFSConfigKeys.NntopNumUsersKey, NTopUsers);
			manager = new RollingWindowManager(conf, WindowLenMs);
			users = new string[2 * NTopUsers];
			for (int i = 0; i < users.Length; i++)
			{
				users[i] = "user" + i;
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestTops()
		{
			long time = WindowLenMs + BucketLen * 3 / 2;
			for (int i = 0; i < users.Length; i++)
			{
				manager.RecordMetric(time, "open", users[i], (i + 1) * 2);
			}
			time++;
			for (int i_1 = 0; i_1 < users.Length; i_1++)
			{
				manager.RecordMetric(time, "close", users[i_1], i_1 + 1);
			}
			time++;
			RollingWindowManager.TopWindow tops = manager.Snapshot(time);
			NUnit.Framework.Assert.AreEqual("Unexpected number of ops", 2, tops.GetOps().Count
				);
			foreach (RollingWindowManager.OP op in tops.GetOps())
			{
				IList<RollingWindowManager.User> topUsers = op.GetTopUsers();
				NUnit.Framework.Assert.AreEqual("Unexpected number of users", NTopUsers, topUsers
					.Count);
				if (op.GetOpType() == "open")
				{
					for (int i_2 = 0; i_2 < topUsers.Count; i_2++)
					{
						RollingWindowManager.User user = topUsers[i_2];
						NUnit.Framework.Assert.AreEqual("Unexpected count for user " + user.GetUser(), (users
							.Length - i_2) * 2, user.GetCount());
					}
					// Closed form of sum(range(2,42,2))
					NUnit.Framework.Assert.AreEqual("Unexpected total count for op", (2 + (users.Length
						 * 2)) * (users.Length / 2), op.GetTotalCount());
				}
			}
			// move the window forward not to see the "open" results
			time += WindowLenMs - 2;
			tops = manager.Snapshot(time);
			NUnit.Framework.Assert.AreEqual("Unexpected number of ops", 1, tops.GetOps().Count
				);
			RollingWindowManager.OP op_1 = tops.GetOps()[0];
			NUnit.Framework.Assert.AreEqual("Should only see close ops", "close", op_1.GetOpType
				());
			IList<RollingWindowManager.User> topUsers_1 = op_1.GetTopUsers();
			for (int i_3 = 0; i_3 < topUsers_1.Count; i_3++)
			{
				RollingWindowManager.User user = topUsers_1[i_3];
				NUnit.Framework.Assert.AreEqual("Unexpected count for user " + user.GetUser(), (users
					.Length - i_3), user.GetCount());
			}
			// Closed form of sum(range(1,21))
			NUnit.Framework.Assert.AreEqual("Unexpected total count for op", (1 + users.Length
				) * (users.Length / 2), op_1.GetTotalCount());
		}
	}
}
