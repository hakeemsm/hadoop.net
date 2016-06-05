using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service
{
	public abstract class Instrumentation
	{
		public interface Cron
		{
			Instrumentation.Cron Start();

			Instrumentation.Cron Stop();
		}

		public interface Variable<T>
		{
			T GetValue();
		}

		public abstract Instrumentation.Cron CreateCron();

		public abstract void Incr(string group, string name, long count);

		public abstract void AddCron(string group, string name, Instrumentation.Cron cron
			);

		public abstract void AddVariable<_T0>(string group, string name, Instrumentation.Variable
			<_T0> variable);

		//sampling happens once a second
		public abstract void AddSampler(string group, string name, int samplingSize, Instrumentation.Variable
			<long> variable);

		public abstract IDictionary<string, IDictionary<string, object>> GetSnapshot();
	}

	public static class InstrumentationConstants
	{
	}
}
