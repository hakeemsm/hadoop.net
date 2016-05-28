using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface CounterGroup
	{
		string GetName();

		string GetDisplayName();

		IDictionary<string, Counter> GetAllCounters();

		Counter GetCounter(string key);

		void SetName(string name);

		void SetDisplayName(string displayName);

		void AddAllCounters(IDictionary<string, Counter> counters);

		void SetCounter(string key, Counter value);

		void RemoveCounter(string key);

		void ClearCounters();
	}
}
