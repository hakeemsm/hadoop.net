using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel
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
	public class FieldSelectionHelper
	{
		public static Text emptyText = new Text(string.Empty);

		public const string DataFieldSeperator = "mapreduce.fieldsel.data.field.separator";

		public const string MapOutputKeyValueSpec = "mapreduce.fieldsel.map.output.key.value.fields.spec";

		public const string ReduceOutputKeyValueSpec = "mapreduce.fieldsel.reduce.output.key.value.fields.spec";

		/// <summary>Extract the actual field numbers from the given field specs.</summary>
		/// <remarks>
		/// Extract the actual field numbers from the given field specs.
		/// If a field spec is in the form of "n-" (like 3-), then n will be the
		/// return value. Otherwise, -1 will be returned.
		/// </remarks>
		/// <param name="fieldListSpec">an array of field specs</param>
		/// <param name="fieldList">an array of field numbers extracted from the specs.</param>
		/// <returns>number n if some field spec is in the form of "n-", -1 otherwise.</returns>
		private static int ExtractFields(string[] fieldListSpec, IList<int> fieldList)
		{
			int allFieldsFrom = -1;
			int i = 0;
			int j = 0;
			int pos = -1;
			string fieldSpec = null;
			for (i = 0; i < fieldListSpec.Length; i++)
			{
				fieldSpec = fieldListSpec[i];
				if (fieldSpec.Length == 0)
				{
					continue;
				}
				pos = fieldSpec.IndexOf('-');
				if (pos < 0)
				{
					int fn = Sharpen.Extensions.ValueOf(fieldSpec);
					fieldList.AddItem(fn);
				}
				else
				{
					string start = Sharpen.Runtime.Substring(fieldSpec, 0, pos);
					string end = Sharpen.Runtime.Substring(fieldSpec, pos + 1);
					if (start.Length == 0)
					{
						start = "0";
					}
					if (end.Length == 0)
					{
						allFieldsFrom = System.Convert.ToInt32(start);
						continue;
					}
					int startPos = System.Convert.ToInt32(start);
					int endPos = System.Convert.ToInt32(end);
					for (j = startPos; j <= endPos; j++)
					{
						fieldList.AddItem(j);
					}
				}
			}
			return allFieldsFrom;
		}

		private static string SelectFields(string[] fields, IList<int> fieldList, int allFieldsFrom
			, string separator)
		{
			string retv = null;
			int i = 0;
			StringBuilder sb = null;
			if (fieldList != null && fieldList.Count > 0)
			{
				if (sb == null)
				{
					sb = new StringBuilder();
				}
				foreach (int index in fieldList)
				{
					if (index < fields.Length)
					{
						sb.Append(fields[index]);
					}
					sb.Append(separator);
				}
			}
			if (allFieldsFrom >= 0)
			{
				if (sb == null)
				{
					sb = new StringBuilder();
				}
				for (i = allFieldsFrom; i < fields.Length; i++)
				{
					sb.Append(fields[i]).Append(separator);
				}
			}
			if (sb != null)
			{
				retv = sb.ToString();
				if (retv.Length > 0)
				{
					retv = Sharpen.Runtime.Substring(retv, 0, retv.Length - 1);
				}
			}
			return retv;
		}

		public static int ParseOutputKeyValueSpec(string keyValueSpec, IList<int> keyFieldList
			, IList<int> valueFieldList)
		{
			string[] keyValSpecs = keyValueSpec.Split(":", -1);
			string[] keySpec = keyValSpecs[0].Split(",");
			string[] valSpec = new string[0];
			if (keyValSpecs.Length > 1)
			{
				valSpec = keyValSpecs[1].Split(",");
			}
			Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel.FieldSelectionHelper.ExtractFields(keySpec
				, keyFieldList);
			return Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel.FieldSelectionHelper.ExtractFields
				(valSpec, valueFieldList);
		}

		public static string SpecToString(string fieldSeparator, string keyValueSpec, int
			 allValueFieldsFrom, IList<int> keyFieldList, IList<int> valueFieldList)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("fieldSeparator: ").Append(fieldSeparator).Append("\n");
			sb.Append("keyValueSpec: ").Append(keyValueSpec).Append("\n");
			sb.Append("allValueFieldsFrom: ").Append(allValueFieldsFrom);
			sb.Append("\n");
			sb.Append("keyFieldList.length: ").Append(keyFieldList.Count);
			sb.Append("\n");
			foreach (int field in keyFieldList)
			{
				sb.Append("\t").Append(field).Append("\n");
			}
			sb.Append("valueFieldList.length: ").Append(valueFieldList.Count);
			sb.Append("\n");
			foreach (int field_1 in valueFieldList)
			{
				sb.Append("\t").Append(field_1).Append("\n");
			}
			return sb.ToString();
		}

		private Org.Apache.Hadoop.IO.Text key = null;

		private Org.Apache.Hadoop.IO.Text value = null;

		public FieldSelectionHelper()
		{
		}

		public FieldSelectionHelper(Org.Apache.Hadoop.IO.Text key, Org.Apache.Hadoop.IO.Text
			 val)
		{
			this.key = key;
			this.value = val;
		}

		public virtual Org.Apache.Hadoop.IO.Text GetKey()
		{
			return key;
		}

		public virtual Org.Apache.Hadoop.IO.Text GetValue()
		{
			return value;
		}

		public virtual void ExtractOutputKeyValue(string key, string val, string fieldSep
			, IList<int> keyFieldList, IList<int> valFieldList, int allValueFieldsFrom, bool
			 ignoreKey, bool isMap)
		{
			if (!ignoreKey)
			{
				val = key + val;
			}
			string[] fields = val.Split(fieldSep);
			string newKey = SelectFields(fields, keyFieldList, -1, fieldSep);
			string newVal = SelectFields(fields, valFieldList, allValueFieldsFrom, fieldSep);
			if (isMap && newKey == null)
			{
				newKey = newVal;
				newVal = null;
			}
			if (newKey != null)
			{
				this.key = new Org.Apache.Hadoop.IO.Text(newKey);
			}
			if (newVal != null)
			{
				this.value = new Org.Apache.Hadoop.IO.Text(newVal);
			}
		}
	}
}
