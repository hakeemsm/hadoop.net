using System;
using System.IO;
using Org.Apache.Avro;
using Org.Apache.Avro.IO;
using Org.Apache.Avro.Specific;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class EventReader : IDisposable
	{
		private string version;

		private Schema schema;

		private DataInputStream @in;

		private Decoder decoder;

		private DatumReader reader;

		/// <summary>Create a new Event Reader</summary>
		/// <param name="fs"/>
		/// <param name="name"/>
		/// <exception cref="System.IO.IOException"/>
		public EventReader(FileSystem fs, Path name)
			: this(fs.Open(name))
		{
		}

		/// <summary>Create a new Event Reader</summary>
		/// <param name="in"/>
		/// <exception cref="System.IO.IOException"/>
		public EventReader(DataInputStream @in)
		{
			this.@in = @in;
			this.version = @in.ReadLine();
			if (!EventWriter.Version.Equals(version))
			{
				throw new IOException("Incompatible event log version: " + version);
			}
			Schema myschema = new SpecificData(typeof(Event).GetClassLoader()).GetSchema(typeof(
				Event));
			this.schema = Schema.Parse(@in.ReadLine());
			this.reader = new SpecificDatumReader(schema, myschema);
			this.decoder = DecoderFactory.Get().JsonDecoder(schema, @in);
		}

		/// <summary>Get the next event from the stream</summary>
		/// <returns>the next event</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual HistoryEvent GetNextEvent()
		{
			Event wrapper;
			try
			{
				wrapper = (Event)reader.Read(null, decoder);
			}
			catch (EOFException)
			{
				// at EOF
				return null;
			}
			HistoryEvent result;
			switch (wrapper.type)
			{
				case EventType.JobSubmitted:
				{
					result = new JobSubmittedEvent();
					break;
				}

				case EventType.JobInited:
				{
					result = new JobInitedEvent();
					break;
				}

				case EventType.JobFinished:
				{
					result = new JobFinishedEvent();
					break;
				}

				case EventType.JobPriorityChanged:
				{
					result = new JobPriorityChangeEvent();
					break;
				}

				case EventType.JobQueueChanged:
				{
					result = new JobQueueChangeEvent();
					break;
				}

				case EventType.JobStatusChanged:
				{
					result = new JobStatusChangedEvent();
					break;
				}

				case EventType.JobFailed:
				{
					result = new JobUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.JobKilled:
				{
					result = new JobUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.JobError:
				{
					result = new JobUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.JobInfoChanged:
				{
					result = new JobInfoChangeEvent();
					break;
				}

				case EventType.TaskStarted:
				{
					result = new TaskStartedEvent();
					break;
				}

				case EventType.TaskFinished:
				{
					result = new TaskFinishedEvent();
					break;
				}

				case EventType.TaskFailed:
				{
					result = new TaskFailedEvent();
					break;
				}

				case EventType.TaskUpdated:
				{
					result = new TaskUpdatedEvent();
					break;
				}

				case EventType.MapAttemptStarted:
				{
					result = new TaskAttemptStartedEvent();
					break;
				}

				case EventType.MapAttemptFinished:
				{
					result = new MapAttemptFinishedEvent();
					break;
				}

				case EventType.MapAttemptFailed:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.MapAttemptKilled:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.ReduceAttemptStarted:
				{
					result = new TaskAttemptStartedEvent();
					break;
				}

				case EventType.ReduceAttemptFinished:
				{
					result = new ReduceAttemptFinishedEvent();
					break;
				}

				case EventType.ReduceAttemptFailed:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.ReduceAttemptKilled:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.SetupAttemptStarted:
				{
					result = new TaskAttemptStartedEvent();
					break;
				}

				case EventType.SetupAttemptFinished:
				{
					result = new TaskAttemptFinishedEvent();
					break;
				}

				case EventType.SetupAttemptFailed:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.SetupAttemptKilled:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.CleanupAttemptStarted:
				{
					result = new TaskAttemptStartedEvent();
					break;
				}

				case EventType.CleanupAttemptFinished:
				{
					result = new TaskAttemptFinishedEvent();
					break;
				}

				case EventType.CleanupAttemptFailed:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.CleanupAttemptKilled:
				{
					result = new TaskAttemptUnsuccessfulCompletionEvent();
					break;
				}

				case EventType.AmStarted:
				{
					result = new AMStartedEvent();
					break;
				}

				default:
				{
					throw new RuntimeException("unexpected event type: " + wrapper.type);
				}
			}
			result.SetDatum(wrapper.@event);
			return result;
		}

		/// <summary>Close the Event reader</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (@in != null)
			{
				@in.Close();
			}
			@in = null;
		}

		internal static Counters FromAvro(JhCounters counters)
		{
			Counters result = new Counters();
			if (counters != null)
			{
				foreach (JhCounterGroup g in counters.groups)
				{
					CounterGroup group = result.AddGroup(StringInterner.WeakIntern(g.name.ToString())
						, StringInterner.WeakIntern(g.displayName.ToString()));
					foreach (JhCounter c in g.counts)
					{
						group.AddCounter(StringInterner.WeakIntern(c.name.ToString()), StringInterner.WeakIntern
							(c.displayName.ToString()), c.value);
					}
				}
			}
			return result;
		}
	}
}
