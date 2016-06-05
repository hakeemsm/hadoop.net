using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface Counters
	{
		IDictionary<string, CounterGroup> GetAllCounterGroups();

		CounterGroup GetCounterGroup(string key);

		Counter GetCounter<_T0>(Enum<_T0> key)
			where _T0 : Enum<E>;

		void AddAllCounterGroups(IDictionary<string, CounterGroup> counterGroups);

		void SetCounterGroup(string key, CounterGroup value);

		void RemoveCounterGroup(string key);

		void ClearCounterGroups();

		void IncrCounter<_T0>(Enum<_T0> key, long amount)
			where _T0 : Enum<E>;
	}
}
