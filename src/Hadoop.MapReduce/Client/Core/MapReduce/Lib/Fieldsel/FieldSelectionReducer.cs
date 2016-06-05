using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel
{
	/// <summary>
	/// This class implements a reducer class that can be used to perform field
	/// selections in a manner similar to unix cut.
	/// </summary>
	/// <remarks>
	/// This class implements a reducer class that can be used to perform field
	/// selections in a manner similar to unix cut.
	/// The input data is treated as fields separated by a user specified
	/// separator (the default value is "\t"). The user can specify a list of
	/// fields that form the reduce output keys, and a list of fields that form
	/// the reduce output values. The fields are the union of those from the key
	/// and those from the value.
	/// The field separator is under attribute "mapreduce.fieldsel.data.field.separator"
	/// The reduce output field list spec is under attribute
	/// "mapreduce.fieldsel.reduce.output.key.value.fields.spec".
	/// The value is expected to be like
	/// "keyFieldsSpec:valueFieldsSpec" key/valueFieldsSpec are comma (,)
	/// separated field spec: fieldSpec,fieldSpec,fieldSpec ... Each field spec
	/// can be a simple number (e.g. 5) specifying a specific field, or a range
	/// (like 2-5) to specify a range of fields, or an open range (like 3-)
	/// specifying all the fields starting from field 3. The open range field
	/// spec applies value fields only. They have no effect on the key fields.
	/// Here is an example: "4,3,0,1:6,5,1-3,7-". It specifies to use fields
	/// 4,3,0 and 1 for keys, and use fields 6,5,1,2,3,7 and above for values.
	/// </remarks>
	public class FieldSelectionReducer<K, V> : Reducer<Text, Text, Text, Text>
	{
		private string fieldSeparator = "\t";

		private string reduceOutputKeyValueSpec;

		private IList<int> reduceOutputKeyFieldList = new AList<int>();

		private IList<int> reduceOutputValueFieldList = new AList<int>();

		private int allReduceValueFieldsFrom = -1;

		public static readonly Log Log = LogFactory.GetLog("FieldSelectionMapReduce");

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Setup(Reducer.Context context)
		{
			Configuration conf = context.GetConfiguration();
			this.fieldSeparator = conf.Get(FieldSelectionHelper.DataFieldSeperator, "\t");
			this.reduceOutputKeyValueSpec = conf.Get(FieldSelectionHelper.ReduceOutputKeyValueSpec
				, "0-:");
			allReduceValueFieldsFrom = FieldSelectionHelper.ParseOutputKeyValueSpec(reduceOutputKeyValueSpec
				, reduceOutputKeyFieldList, reduceOutputValueFieldList);
			Log.Info(FieldSelectionHelper.SpecToString(fieldSeparator, reduceOutputKeyValueSpec
				, allReduceValueFieldsFrom, reduceOutputKeyFieldList, reduceOutputValueFieldList
				));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Reduce(Text key, IEnumerable<Text> values, Reducer.Context
			 context)
		{
			string keyStr = key.ToString() + this.fieldSeparator;
			foreach (Text val in values)
			{
				FieldSelectionHelper helper = new FieldSelectionHelper();
				helper.ExtractOutputKeyValue(keyStr, val.ToString(), fieldSeparator, reduceOutputKeyFieldList
					, reduceOutputValueFieldList, allReduceValueFieldsFrom, false, false);
				context.Write(helper.GetKey(), helper.GetValue());
			}
		}
	}
}
