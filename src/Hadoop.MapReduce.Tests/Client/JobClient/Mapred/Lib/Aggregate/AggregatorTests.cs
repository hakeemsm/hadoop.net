using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	public class AggregatorTests : ValueAggregatorBaseDescriptor
	{
		public override AList<KeyValuePair<Text, Text>> GenerateKeyValPairs(object key, object
			 val)
		{
			AList<KeyValuePair<Text, Text>> retv = new AList<KeyValuePair<Text, Text>>();
			string[] words = val.ToString().Split(" ");
			string countType;
			string id;
			KeyValuePair<Text, Text> e;
			foreach (string word in words)
			{
				long numVal = long.Parse(word);
				countType = LongValueSum;
				id = "count_" + word;
				e = GenerateEntry(countType, id, ValueAggregatorDescriptor.One);
				if (e != null)
				{
					retv.AddItem(e);
				}
				countType = LongValueMax;
				id = "max";
				e = GenerateEntry(countType, id, new Text(word));
				if (e != null)
				{
					retv.AddItem(e);
				}
				countType = LongValueMin;
				id = "min";
				e = GenerateEntry(countType, id, new Text(word));
				if (e != null)
				{
					retv.AddItem(e);
				}
				countType = StringValueMax;
				id = "value_as_string_max";
				e = GenerateEntry(countType, id, new Text(string.Empty + numVal));
				if (e != null)
				{
					retv.AddItem(e);
				}
				countType = StringValueMin;
				id = "value_as_string_min";
				e = GenerateEntry(countType, id, new Text(string.Empty + numVal));
				if (e != null)
				{
					retv.AddItem(e);
				}
				countType = UniqValueCount;
				id = "uniq_count";
				e = GenerateEntry(countType, id, new Text(word));
				if (e != null)
				{
					retv.AddItem(e);
				}
				countType = ValueHistogram;
				id = "histogram";
				e = GenerateEntry(countType, id, new Text(word));
				if (e != null)
				{
					retv.AddItem(e);
				}
			}
			return retv;
		}
	}
}
