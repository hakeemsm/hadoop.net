using Org.Apache.Avro;
using Org.Apache.Avro.IO;
using Org.Apache.Avro.Specific;
using Org.Apache.Avro.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>
	/// Event Writer is an utility class used to write events to the underlying
	/// stream.
	/// </summary>
	/// <remarks>
	/// Event Writer is an utility class used to write events to the underlying
	/// stream. Typically, one event writer (which translates to one stream)
	/// is created per job
	/// </remarks>
	internal class EventWriter
	{
		internal const string Version = "Avro-Json";

		private FSDataOutputStream @out;

		private DatumWriter<Event> writer = new SpecificDatumWriter<Event>(typeof(Event));

		private Encoder encoder;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Jobhistory.EventWriter
			));

		/// <exception cref="System.IO.IOException"/>
		internal EventWriter(FSDataOutputStream @out)
		{
			this.@out = @out;
			@out.WriteBytes(Version);
			@out.WriteBytes("\n");
			@out.WriteBytes(Event.Schema$.ToString());
			@out.WriteBytes("\n");
			this.encoder = EncoderFactory.Get().JsonEncoder(Event.Schema$, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Write(HistoryEvent @event)
		{
			lock (this)
			{
				Event wrapper = new Event();
				wrapper.type = @event.GetEventType();
				wrapper.@event = @event.GetDatum();
				writer.Write(wrapper, encoder);
				encoder.Flush();
				@out.WriteBytes("\n");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Flush()
		{
			encoder.Flush();
			@out.Flush();
			@out.Hflush();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Close()
		{
			try
			{
				encoder.Flush();
				@out.Close();
				@out = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, @out);
			}
		}

		private static readonly Schema Groups = Schema.CreateArray(JhCounterGroup.Schema$
			);

		private static readonly Schema Counters = Schema.CreateArray(JhCounter.Schema$);

		internal static JhCounters ToAvro(Counters counters)
		{
			return ToAvro(counters, "COUNTERS");
		}

		internal static JhCounters ToAvro(Counters counters, string name)
		{
			JhCounters result = new JhCounters();
			result.name = new Utf8(name);
			result.groups = new AList<JhCounterGroup>(0);
			if (counters == null)
			{
				return result;
			}
			foreach (CounterGroup group in counters)
			{
				JhCounterGroup g = new JhCounterGroup();
				g.name = new Utf8(group.GetName());
				g.displayName = new Utf8(group.GetDisplayName());
				g.counts = new AList<JhCounter>(group.Size());
				foreach (Counter counter in group)
				{
					JhCounter c = new JhCounter();
					c.name = new Utf8(counter.GetName());
					c.displayName = new Utf8(counter.GetDisplayName());
					c.value = counter.GetValue();
					g.counts.AddItem(c);
				}
				result.groups.AddItem(g);
			}
			return result;
		}
	}
}
