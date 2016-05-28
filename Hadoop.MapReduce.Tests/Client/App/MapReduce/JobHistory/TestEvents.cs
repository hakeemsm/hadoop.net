using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class TestEvents
	{
		/// <summary>test a getters of TaskAttemptFinishedEvent and TaskAttemptFinished</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTaskAttemptFinishedEvent()
		{
			JobID jid = new JobID("001", 1);
			TaskID tid = new TaskID(jid, TaskType.Reduce, 2);
			TaskAttemptID taskAttemptId = new TaskAttemptID(tid, 3);
			Counters counters = new Counters();
			TaskAttemptFinishedEvent test = new TaskAttemptFinishedEvent(taskAttemptId, TaskType
				.Reduce, "TEST", 123L, "RAKNAME", "HOSTNAME", "STATUS", counters);
			NUnit.Framework.Assert.AreEqual(test.GetAttemptId().ToString(), taskAttemptId.ToString
				());
			NUnit.Framework.Assert.AreEqual(test.GetCounters(), counters);
			NUnit.Framework.Assert.AreEqual(test.GetFinishTime(), 123L);
			NUnit.Framework.Assert.AreEqual(test.GetHostname(), "HOSTNAME");
			NUnit.Framework.Assert.AreEqual(test.GetRackName(), "RAKNAME");
			NUnit.Framework.Assert.AreEqual(test.GetState(), "STATUS");
			NUnit.Framework.Assert.AreEqual(test.GetTaskId(), tid);
			NUnit.Framework.Assert.AreEqual(test.GetTaskStatus(), "TEST");
			NUnit.Framework.Assert.AreEqual(test.GetTaskType(), TaskType.Reduce);
		}

		/// <summary>simple test JobPriorityChangeEvent and JobPriorityChange</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestJobPriorityChange()
		{
			JobID jid = new JobID("001", 1);
			JobPriorityChangeEvent test = new JobPriorityChangeEvent(jid, JobPriority.Low);
			NUnit.Framework.Assert.AreEqual(test.GetJobId().ToString(), jid.ToString());
			NUnit.Framework.Assert.AreEqual(test.GetPriority(), JobPriority.Low);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobQueueChange()
		{
			JobID jid = new JobID("001", 1);
			JobQueueChangeEvent test = new JobQueueChangeEvent(jid, "newqueue");
			NUnit.Framework.Assert.AreEqual(test.GetJobId().ToString(), jid.ToString());
			NUnit.Framework.Assert.AreEqual(test.GetJobQueueName(), "newqueue");
		}

		/// <summary>simple test TaskUpdatedEvent and TaskUpdated</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTaskUpdated()
		{
			JobID jid = new JobID("001", 1);
			TaskID tid = new TaskID(jid, TaskType.Reduce, 2);
			TaskUpdatedEvent test = new TaskUpdatedEvent(tid, 1234L);
			NUnit.Framework.Assert.AreEqual(test.GetTaskId().ToString(), tid.ToString());
			NUnit.Framework.Assert.AreEqual(test.GetFinishTime(), 1234L);
		}

		/*
		* test EventReader EventReader should read the list of events and return
		* instance of HistoryEvent Different HistoryEvent should have a different
		* datum.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestEvents()
		{
			EventReader reader = new EventReader(new DataInputStream(new ByteArrayInputStream
				(GetEvents())));
			HistoryEvent e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.JobPriorityChanged
				));
			NUnit.Framework.Assert.AreEqual("ID", ((JobPriorityChange)e.GetDatum()).jobid.ToString
				());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.JobStatusChanged)
				);
			NUnit.Framework.Assert.AreEqual("ID", ((JobStatusChanged)e.GetDatum()).jobid.ToString
				());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.TaskUpdated));
			NUnit.Framework.Assert.AreEqual("ID", ((TaskUpdated)e.GetDatum()).taskid.ToString
				());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptKilled
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptUnsuccessfulCompletion
				)e.GetDatum()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.JobKilled));
			NUnit.Framework.Assert.AreEqual("ID", ((JobUnsuccessfulCompletion)e.GetDatum()).jobid
				.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptStarted
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptStarted)e.GetDatum
				()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptFinished
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptFinished)e.GetDatum
				()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptKilled
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptUnsuccessfulCompletion
				)e.GetDatum()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptKilled
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptUnsuccessfulCompletion
				)e.GetDatum()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptStarted
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptStarted)e.GetDatum
				()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptFinished
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptFinished)e.GetDatum
				()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptKilled
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptUnsuccessfulCompletion
				)e.GetDatum()).taskid.ToString());
			e = reader.GetNextEvent();
			NUnit.Framework.Assert.IsTrue(e.GetEventType().Equals(EventType.ReduceAttemptKilled
				));
			NUnit.Framework.Assert.AreEqual("task_1_2_r03_4", ((TaskAttemptUnsuccessfulCompletion
				)e.GetDatum()).taskid.ToString());
			reader.Close();
		}

		/*
		* makes array of bytes with History events
		*/
		/// <exception cref="System.Exception"/>
		private byte[] GetEvents()
		{
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			FSDataOutputStream fsOutput = new FSDataOutputStream(output, new FileSystem.Statistics
				("scheme"));
			EventWriter writer = new EventWriter(fsOutput);
			writer.Write(GetJobPriorityChangedEvent());
			writer.Write(GetJobStatusChangedEvent());
			writer.Write(GetTaskUpdatedEvent());
			writer.Write(GetReduceAttemptKilledEvent());
			writer.Write(GetJobKilledEvent());
			writer.Write(GetSetupAttemptStartedEvent());
			writer.Write(GetTaskAttemptFinishedEvent());
			writer.Write(GetSetupAttemptFieledEvent());
			writer.Write(GetSetupAttemptKilledEvent());
			writer.Write(GetCleanupAttemptStartedEvent());
			writer.Write(GetCleanupAttemptFinishedEvent());
			writer.Write(GetCleanupAttemptFiledEvent());
			writer.Write(GetCleanupAttemptKilledEvent());
			writer.Flush();
			writer.Close();
			return output.ToByteArray();
		}

		private TestEvents.FakeEvent GetCleanupAttemptKilledEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.CleanupAttemptKilled
				);
			result.SetDatum(GetTaskAttemptUnsuccessfulCompletion());
			return result;
		}

		private TestEvents.FakeEvent GetCleanupAttemptFiledEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.CleanupAttemptFailed
				);
			result.SetDatum(GetTaskAttemptUnsuccessfulCompletion());
			return result;
		}

		private TaskAttemptUnsuccessfulCompletion GetTaskAttemptUnsuccessfulCompletion()
		{
			TaskAttemptUnsuccessfulCompletion datum = new TaskAttemptUnsuccessfulCompletion();
			datum.attemptId = "attempt_1_2_r3_4_5";
			datum.clockSplits = Arrays.AsList(1, 2, 3);
			datum.cpuUsages = Arrays.AsList(100, 200, 300);
			datum.error = "Error";
			datum.finishTime = 2;
			datum.hostname = "hostname";
			datum.rackname = "rackname";
			datum.physMemKbytes = Arrays.AsList(1000, 2000, 3000);
			datum.taskid = "task_1_2_r03_4";
			datum.port = 1000;
			datum.taskType = "REDUCE";
			datum.status = "STATUS";
			datum.counters = GetCounters();
			datum.vMemKbytes = Arrays.AsList(1000, 2000, 3000);
			return datum;
		}

		private JhCounters GetCounters()
		{
			JhCounters counters = new JhCounters();
			counters.groups = new AList<JhCounterGroup>(0);
			counters.name = "name";
			return counters;
		}

		private TestEvents.FakeEvent GetCleanupAttemptFinishedEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.CleanupAttemptFinished
				);
			TaskAttemptFinished datum = new TaskAttemptFinished();
			datum.attemptId = "attempt_1_2_r3_4_5";
			datum.counters = GetCounters();
			datum.finishTime = 2;
			datum.hostname = "hostname";
			datum.rackname = "rackName";
			datum.state = "state";
			datum.taskid = "task_1_2_r03_4";
			datum.taskStatus = "taskStatus";
			datum.taskType = "REDUCE";
			result.SetDatum(datum);
			return result;
		}

		private TestEvents.FakeEvent GetCleanupAttemptStartedEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.CleanupAttemptStarted
				);
			TaskAttemptStarted datum = new TaskAttemptStarted();
			datum.attemptId = "attempt_1_2_r3_4_5";
			datum.avataar = "avatar";
			datum.containerId = "containerId";
			datum.httpPort = 10000;
			datum.locality = "locality";
			datum.shufflePort = 10001;
			datum.startTime = 1;
			datum.taskid = "task_1_2_r03_4";
			datum.taskType = "taskType";
			datum.trackerName = "trackerName";
			result.SetDatum(datum);
			return result;
		}

		private TestEvents.FakeEvent GetSetupAttemptKilledEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.SetupAttemptKilled
				);
			result.SetDatum(GetTaskAttemptUnsuccessfulCompletion());
			return result;
		}

		private TestEvents.FakeEvent GetSetupAttemptFieledEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.SetupAttemptFailed
				);
			result.SetDatum(GetTaskAttemptUnsuccessfulCompletion());
			return result;
		}

		private TestEvents.FakeEvent GetTaskAttemptFinishedEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.SetupAttemptFinished
				);
			TaskAttemptFinished datum = new TaskAttemptFinished();
			datum.attemptId = "attempt_1_2_r3_4_5";
			datum.counters = GetCounters();
			datum.finishTime = 2;
			datum.hostname = "hostname";
			datum.rackname = "rackname";
			datum.state = "state";
			datum.taskid = "task_1_2_r03_4";
			datum.taskStatus = "taskStatus";
			datum.taskType = "REDUCE";
			result.SetDatum(datum);
			return result;
		}

		private TestEvents.FakeEvent GetSetupAttemptStartedEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.SetupAttemptStarted
				);
			TaskAttemptStarted datum = new TaskAttemptStarted();
			datum.attemptId = "ID";
			datum.avataar = "avataar";
			datum.containerId = "containerId";
			datum.httpPort = 10000;
			datum.locality = "locality";
			datum.shufflePort = 10001;
			datum.startTime = 1;
			datum.taskid = "task_1_2_r03_4";
			datum.taskType = "taskType";
			datum.trackerName = "trackerName";
			result.SetDatum(datum);
			return result;
		}

		private TestEvents.FakeEvent GetJobKilledEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.JobKilled);
			JobUnsuccessfulCompletion datum = new JobUnsuccessfulCompletion();
			datum.SetFinishedMaps(1);
			datum.SetFinishedReduces(2);
			datum.SetFinishTime(3L);
			datum.SetJobid("ID");
			datum.SetJobStatus("STATUS");
			datum.SetDiagnostics(JobImpl.JobKilledDiag);
			result.SetDatum(datum);
			return result;
		}

		private TestEvents.FakeEvent GetReduceAttemptKilledEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.ReduceAttemptKilled
				);
			result.SetDatum(GetTaskAttemptUnsuccessfulCompletion());
			return result;
		}

		private TestEvents.FakeEvent GetJobPriorityChangedEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.JobPriorityChanged
				);
			JobPriorityChange datum = new JobPriorityChange();
			datum.jobid = "ID";
			datum.priority = "priority";
			result.SetDatum(datum);
			return result;
		}

		private TestEvents.FakeEvent GetJobStatusChangedEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.JobStatusChanged
				);
			JobStatusChanged datum = new JobStatusChanged();
			datum.jobid = "ID";
			datum.jobStatus = "newStatus";
			result.SetDatum(datum);
			return result;
		}

		private TestEvents.FakeEvent GetTaskUpdatedEvent()
		{
			TestEvents.FakeEvent result = new TestEvents.FakeEvent(this, EventType.TaskUpdated
				);
			TaskUpdated datum = new TaskUpdated();
			datum.finishTime = 2;
			datum.taskid = "ID";
			result.SetDatum(datum);
			return result;
		}

		private class FakeEvent : HistoryEvent
		{
			private EventType eventType;

			private object datum;

			public FakeEvent(TestEvents _enclosing, EventType eventType)
			{
				this._enclosing = _enclosing;
				this.eventType = eventType;
			}

			public virtual EventType GetEventType()
			{
				return this.eventType;
			}

			public virtual object GetDatum()
			{
				return this.datum;
			}

			public virtual void SetDatum(object datum)
			{
				this.datum = datum;
			}

			private readonly TestEvents _enclosing;
		}
	}
}
