using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel
{
	/// <summary>
	/// This class implements a mapper class that can be used to perform
	/// field selections in a manner similar to unix cut.
	/// </summary>
	/// <remarks>
	/// This class implements a mapper class that can be used to perform
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
	/// The value is expected to be like
	/// "keyFieldsSpec:valueFieldsSpec" key/valueFieldsSpec are comma (,) separated
	/// field spec: fieldSpec,fieldSpec,fieldSpec ... Each field spec can be a
	/// simple number (e.g. 5) specifying a specific field, or a range (like 2-5)
	/// to specify a range of fields, or an open range (like 3-) specifying all
	/// the fields starting from field 3. The open range field spec applies value
	/// fields only. They have no effect on the key fields.
	/// Here is an example: "4,3,0,1:6,5,1-3,7-". It specifies to use fields
	/// 4,3,0 and 1 for keys, and use fields 6,5,1,2,3,7 and above for values.
	/// </remarks>
	public class FieldSelectionMapper<K, V> : Mapper<K, V, Text, Text>
	{
		private string mapOutputKeyValueSpec;

		private bool ignoreInputKey;

		private string fieldSeparator = "\t";

		private IList<int> mapOutputKeyFieldList = new AList<int>();

		private IList<int> mapOutputValueFieldList = new AList<int>();

		private int allMapValueFieldsFrom = -1;

		public static readonly Log Log = LogFactory.GetLog("FieldSelectionMapReduce");

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Setup(Mapper.Context context)
		{
			Configuration conf = context.GetConfiguration();
			this.fieldSeparator = conf.Get(FieldSelectionHelper.DataFieldSeperator, "\t");
			this.mapOutputKeyValueSpec = conf.Get(FieldSelectionHelper.MapOutputKeyValueSpec, 
				"0-:");
			try
			{
				this.ignoreInputKey = typeof(TextInputFormat).GetCanonicalName().Equals(context.GetInputFormatClass
					().GetCanonicalName());
			}
			catch (TypeLoadException e)
			{
				throw new IOException("Input format class not found", e);
			}
			allMapValueFieldsFrom = FieldSelectionHelper.ParseOutputKeyValueSpec(mapOutputKeyValueSpec
				, mapOutputKeyFieldList, mapOutputValueFieldList);
			Log.Info(FieldSelectionHelper.SpecToString(fieldSeparator, mapOutputKeyValueSpec, 
				allMapValueFieldsFrom, mapOutputKeyFieldList, mapOutputValueFieldList) + "\nignoreInputKey:"
				 + ignoreInputKey);
		}

		/// <summary>The identify function.</summary>
		/// <remarks>The identify function. Input key/value pair is written directly to output.
		/// 	</remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Map(K key, V val, Mapper.Context context)
		{
			FieldSelectionHelper helper = new FieldSelectionHelper(FieldSelectionHelper.emptyText
				, FieldSelectionHelper.emptyText);
			helper.ExtractOutputKeyValue(key.ToString(), val.ToString(), fieldSeparator, mapOutputKeyFieldList
				, mapOutputValueFieldList, allMapValueFieldsFrom, ignoreInputKey, true);
			context.Write(helper.GetKey(), helper.GetValue());
		}
	}
}
