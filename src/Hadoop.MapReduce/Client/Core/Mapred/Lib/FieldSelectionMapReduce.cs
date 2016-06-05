using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// This class implements a mapper/reducer class that can be used to perform
	/// field selections in a manner similar to unix cut.
	/// </summary>
	/// <remarks>
	/// This class implements a mapper/reducer class that can be used to perform
	/// field selections in a manner similar to unix cut. The input data is treated
	/// as fields separated by a user specified separator (the default value is
	/// "\t"). The user can specify a list of fields that form the map output keys,
	/// and a list of fields that form the map output values. If the inputformat is
	/// TextInputFormat, the mapper will ignore the key to the map function. and the
	/// fields are from the value only. Otherwise, the fields are the union of those
	/// from the key and those from the value.
	/// The field separator is under attribute "mapreduce.fieldsel.data.field.separator"
	/// The map output field list spec is under attribute
	/// "mapreduce.fieldsel.map.output.key.value.fields.spec".
	/// The value is expected to be like "keyFieldsSpec:valueFieldsSpec"
	/// key/valueFieldsSpec are comma (,) separated field spec: fieldSpec,fieldSpec,fieldSpec ...
	/// Each field spec can be a simple number (e.g. 5) specifying a specific field, or a range
	/// (like 2-5) to specify a range of fields, or an open range (like 3-) specifying all
	/// the fields starting from field 3. The open range field spec applies value fields only.
	/// They have no effect on the key fields.
	/// Here is an example: "4,3,0,1:6,5,1-3,7-". It specifies to use fields 4,3,0 and 1 for keys,
	/// and use fields 6,5,1,2,3,7 and above for values.
	/// The reduce output field list spec is under attribute
	/// "mapreduce.fieldsel.reduce.output.key.value.fields.spec".
	/// The reducer extracts output key/value pairs in a similar manner, except that
	/// the key is never ignored.
	/// </remarks>
	public class FieldSelectionMapReduce<K, V> : Mapper<K, V, Text, Text>, Reducer<Text
		, Text, Text, Text>
	{
		private string mapOutputKeyValueSpec;

		private bool ignoreInputKey;

		private string fieldSeparator = "\t";

		private IList<int> mapOutputKeyFieldList = new AList<int>();

		private IList<int> mapOutputValueFieldList = new AList<int>();

		private int allMapValueFieldsFrom = -1;

		private string reduceOutputKeyValueSpec;

		private IList<int> reduceOutputKeyFieldList = new AList<int>();

		private IList<int> reduceOutputValueFieldList = new AList<int>();

		private int allReduceValueFieldsFrom = -1;

		public static readonly Log Log = LogFactory.GetLog("FieldSelectionMapReduce");

		private string SpecToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("fieldSeparator: ").Append(fieldSeparator).Append("\n");
			sb.Append("mapOutputKeyValueSpec: ").Append(mapOutputKeyValueSpec).Append("\n");
			sb.Append("reduceOutputKeyValueSpec: ").Append(reduceOutputKeyValueSpec).Append("\n"
				);
			sb.Append("allMapValueFieldsFrom: ").Append(allMapValueFieldsFrom).Append("\n");
			sb.Append("allReduceValueFieldsFrom: ").Append(allReduceValueFieldsFrom).Append("\n"
				);
			int i = 0;
			sb.Append("mapOutputKeyFieldList.length: ").Append(mapOutputKeyFieldList.Count).Append
				("\n");
			for (i = 0; i < mapOutputKeyFieldList.Count; i++)
			{
				sb.Append("\t").Append(mapOutputKeyFieldList[i]).Append("\n");
			}
			sb.Append("mapOutputValueFieldList.length: ").Append(mapOutputValueFieldList.Count
				).Append("\n");
			for (i = 0; i < mapOutputValueFieldList.Count; i++)
			{
				sb.Append("\t").Append(mapOutputValueFieldList[i]).Append("\n");
			}
			sb.Append("reduceOutputKeyFieldList.length: ").Append(reduceOutputKeyFieldList.Count
				).Append("\n");
			for (i = 0; i < reduceOutputKeyFieldList.Count; i++)
			{
				sb.Append("\t").Append(reduceOutputKeyFieldList[i]).Append("\n");
			}
			sb.Append("reduceOutputValueFieldList.length: ").Append(reduceOutputValueFieldList
				.Count).Append("\n");
			for (i = 0; i < reduceOutputValueFieldList.Count; i++)
			{
				sb.Append("\t").Append(reduceOutputValueFieldList[i]).Append("\n");
			}
			return sb.ToString();
		}

		/// <summary>The identify function.</summary>
		/// <remarks>The identify function. Input key/value pair is written directly to output.
		/// 	</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(K key, V val, OutputCollector<Org.Apache.Hadoop.IO.Text, 
			Org.Apache.Hadoop.IO.Text> output, Reporter reporter)
		{
			FieldSelectionHelper helper = new FieldSelectionHelper(FieldSelectionHelper.emptyText
				, FieldSelectionHelper.emptyText);
			helper.ExtractOutputKeyValue(key.ToString(), val.ToString(), fieldSeparator, mapOutputKeyFieldList
				, mapOutputValueFieldList, allMapValueFieldsFrom, ignoreInputKey, true);
			output.Collect(helper.GetKey(), helper.GetValue());
		}

		private void ParseOutputKeyValueSpec()
		{
			allMapValueFieldsFrom = FieldSelectionHelper.ParseOutputKeyValueSpec(mapOutputKeyValueSpec
				, mapOutputKeyFieldList, mapOutputValueFieldList);
			allReduceValueFieldsFrom = FieldSelectionHelper.ParseOutputKeyValueSpec(reduceOutputKeyValueSpec
				, reduceOutputKeyFieldList, reduceOutputValueFieldList);
		}

		public virtual void Configure(JobConf job)
		{
			this.fieldSeparator = job.Get(FieldSelectionHelper.DataFieldSeperator, "\t");
			this.mapOutputKeyValueSpec = job.Get(FieldSelectionHelper.MapOutputKeyValueSpec, 
				"0-:");
			this.ignoreInputKey = typeof(TextInputFormat).GetCanonicalName().Equals(job.GetInputFormat
				().GetType().GetCanonicalName());
			this.reduceOutputKeyValueSpec = job.Get(FieldSelectionHelper.ReduceOutputKeyValueSpec
				, "0-:");
			ParseOutputKeyValueSpec();
			Log.Info(SpecToString());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
		}

		// TODO Auto-generated method stub
		/// <exception cref="System.IO.IOException"/>
		public virtual void Reduce(Org.Apache.Hadoop.IO.Text key, IEnumerator<Org.Apache.Hadoop.IO.Text
			> values, OutputCollector<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> 
			output, Reporter reporter)
		{
			string keyStr = key.ToString() + this.fieldSeparator;
			while (values.HasNext())
			{
				FieldSelectionHelper helper = new FieldSelectionHelper();
				helper.ExtractOutputKeyValue(keyStr, values.Next().ToString(), fieldSeparator, reduceOutputKeyFieldList
					, reduceOutputValueFieldList, allReduceValueFieldsFrom, false, false);
				output.Collect(helper.GetKey(), helper.GetValue());
			}
		}
	}
}
