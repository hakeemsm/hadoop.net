using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestTaskStatus
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestTaskStatus));

		[NUnit.Framework.Test]
		public virtual void TestMapTaskStatusStartAndFinishTimes()
		{
			CheckTaskStatues(true);
		}

		[NUnit.Framework.Test]
		public virtual void TestReduceTaskStatusStartAndFinishTimes()
		{
			CheckTaskStatues(false);
		}

		/// <summary>
		/// Private utility method which ensures uniform testing of newly created
		/// TaskStatus object.
		/// </summary>
		/// <param name="isMap">true to test map task status, false for reduce.</param>
		private void CheckTaskStatues(bool isMap)
		{
			TaskStatus status = null;
			if (isMap)
			{
				status = new MapTaskStatus();
			}
			else
			{
				status = new ReduceTaskStatus();
			}
			long currentTime = Runtime.CurrentTimeMillis();
			// first try to set the finish time before
			// start time is set.
			status.SetFinishTime(currentTime);
			NUnit.Framework.Assert.AreEqual("Finish time of the task status set without start time"
				, 0, status.GetFinishTime());
			// Now set the start time to right time.
			status.SetStartTime(currentTime);
			NUnit.Framework.Assert.AreEqual("Start time of the task status not set correctly."
				, currentTime, status.GetStartTime());
			// try setting wrong start time to task status.
			long wrongTime = -1;
			status.SetStartTime(wrongTime);
			NUnit.Framework.Assert.AreEqual("Start time of the task status is set to wrong negative value"
				, currentTime, status.GetStartTime());
			// finally try setting wrong finish time i.e. negative value.
			status.SetFinishTime(wrongTime);
			NUnit.Framework.Assert.AreEqual("Finish time of task status is set to wrong negative value"
				, 0, status.GetFinishTime());
			status.SetFinishTime(currentTime);
			NUnit.Framework.Assert.AreEqual("Finish time of the task status not set correctly."
				, currentTime, status.GetFinishTime());
			// test with null task-diagnostics
			TaskStatus ts = ((TaskStatus)status.Clone());
			ts.SetDiagnosticInfo(null);
			ts.SetDiagnosticInfo(string.Empty);
			ts.SetStateString(null);
			ts.SetStateString(string.Empty);
			((TaskStatus)status.Clone()).StatusUpdate(ts);
			// test with null state-string
			((TaskStatus)status.Clone()).StatusUpdate(0, null, null);
			((TaskStatus)status.Clone()).StatusUpdate(0, string.Empty, null);
			((TaskStatus)status.Clone()).StatusUpdate(null, 0, string.Empty, null, 1);
		}

		/// <summary>
		/// Test the
		/// <see cref="TaskStatus"/>
		/// against large sized task-diagnostic-info and
		/// state-string. Does the following
		/// - create Map/Reduce TaskStatus such that the task-diagnostic-info and
		/// state-string are small strings and check their contents
		/// - append them with small string and check their contents
		/// - append them with large string and check their size
		/// - update the status using statusUpdate() calls and check the size/contents
		/// - create Map/Reduce TaskStatus with large string and check their size
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestTaskDiagnosticsAndStateString()
		{
			// check the default case
			string test = "hi";
			int maxSize = 16;
			TaskStatus status = new _TaskStatus_107(maxSize, null, 0, 0, null, test, test, null
				, null, null);
			NUnit.Framework.Assert.AreEqual("Small diagnostic info test failed", status.GetDiagnosticInfo
				(), test);
			NUnit.Framework.Assert.AreEqual("Small state string test failed", status.GetStateString
				(), test);
			// now append some small string and check
			string newDInfo = test.Concat(test);
			status.SetDiagnosticInfo(test);
			status.SetStateString(newDInfo);
			NUnit.Framework.Assert.AreEqual("Small diagnostic info append failed", newDInfo, 
				status.GetDiagnosticInfo());
			NUnit.Framework.Assert.AreEqual("Small state-string append failed", newDInfo, status
				.GetStateString());
			// update the status with small state strings
			TaskStatus newStatus = (TaskStatus)status.Clone();
			string newSInfo = "hi1";
			newStatus.SetStateString(newSInfo);
			status.StatusUpdate(newStatus);
			newDInfo = newDInfo.Concat(newStatus.GetDiagnosticInfo());
			NUnit.Framework.Assert.AreEqual("Status-update on diagnostic-info failed", newDInfo
				, status.GetDiagnosticInfo());
			NUnit.Framework.Assert.AreEqual("Status-update on state-string failed", newSInfo, 
				status.GetStateString());
			newSInfo = "hi2";
			status.StatusUpdate(0, newSInfo, null);
			NUnit.Framework.Assert.AreEqual("Status-update on state-string failed", newSInfo, 
				status.GetStateString());
			newSInfo = "hi3";
			status.StatusUpdate(null, 0, newSInfo, null, 0);
			NUnit.Framework.Assert.AreEqual("Status-update on state-string failed", newSInfo, 
				status.GetStateString());
			// now append each with large string
			string large = "hihihihihihihihihihi";
			// 20 chars
			status.SetDiagnosticInfo(large);
			status.SetStateString(large);
			NUnit.Framework.Assert.AreEqual("Large diagnostic info append test failed", maxSize
				, status.GetDiagnosticInfo().Length);
			NUnit.Framework.Assert.AreEqual("Large state-string append test failed", maxSize, 
				status.GetStateString().Length);
			// update a large status with large strings
			newStatus.SetDiagnosticInfo(large + "0");
			newStatus.SetStateString(large + "1");
			status.StatusUpdate(newStatus);
			NUnit.Framework.Assert.AreEqual("Status-update on diagnostic info failed", maxSize
				, status.GetDiagnosticInfo().Length);
			NUnit.Framework.Assert.AreEqual("Status-update on state-string failed", maxSize, 
				status.GetStateString().Length);
			status.StatusUpdate(0, large + "2", null);
			NUnit.Framework.Assert.AreEqual("Status-update on state-string failed", maxSize, 
				status.GetStateString().Length);
			status.StatusUpdate(null, 0, large + "3", null, 0);
			NUnit.Framework.Assert.AreEqual("Status-update on state-string failed", maxSize, 
				status.GetStateString().Length);
			// test passing large string in constructor
			status = new _TaskStatus_187(maxSize, null, 0, 0, null, large, large, null, null, 
				null);
			NUnit.Framework.Assert.AreEqual("Large diagnostic info test failed", maxSize, status
				.GetDiagnosticInfo().Length);
			NUnit.Framework.Assert.AreEqual("Large state-string test failed", maxSize, status
				.GetStateString().Length);
		}

		private sealed class _TaskStatus_107 : TaskStatus
		{
			public _TaskStatus_107(int maxSize, TaskAttemptID baseArg1, float baseArg2, int baseArg3
				, TaskStatus.State baseArg4, string baseArg5, string baseArg6, string baseArg7, 
				TaskStatus.Phase baseArg8, Counters baseArg9)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9)
			{
				this.maxSize = maxSize;
			}

			protected override int GetMaxStringSize()
			{
				return maxSize;
			}

			public override void AddFetchFailedMap(TaskAttemptID mapTaskId)
			{
			}

			public override bool GetIsMap()
			{
				return false;
			}

			private readonly int maxSize;
		}

		private sealed class _TaskStatus_187 : TaskStatus
		{
			public _TaskStatus_187(int maxSize, TaskAttemptID baseArg1, float baseArg2, int baseArg3
				, TaskStatus.State baseArg4, string baseArg5, string baseArg6, string baseArg7, 
				TaskStatus.Phase baseArg8, Counters baseArg9)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9)
			{
				this.maxSize = maxSize;
			}

			protected override int GetMaxStringSize()
			{
				return maxSize;
			}

			public override void AddFetchFailedMap(TaskAttemptID mapTaskId)
			{
			}

			public override bool GetIsMap()
			{
				return false;
			}

			private readonly int maxSize;
		}
	}
}
