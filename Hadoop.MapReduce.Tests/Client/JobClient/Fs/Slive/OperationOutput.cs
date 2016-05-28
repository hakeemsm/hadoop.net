using System;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// An operation output has the following object format whereby simple types are
	/// represented as a key of dataType:operationType*measurementType and these
	/// simple types can be combined (mainly in the reducer) using there given types
	/// into a single operation output.
	/// </summary>
	/// <remarks>
	/// An operation output has the following object format whereby simple types are
	/// represented as a key of dataType:operationType*measurementType and these
	/// simple types can be combined (mainly in the reducer) using there given types
	/// into a single operation output.
	/// Combination is done based on the data types and the following convention is
	/// followed (in the following order). If one is a string then the other will be
	/// concated as a string with a ";" separator. If one is a double then the other
	/// will be added as a double and the output will be a double. If one is a float
	/// then the other will be added as a float and the the output will be a float.
	/// Following this if one is a long the other will be added as a long and the
	/// output type will be a long and if one is a integer the other will be added as
	/// a integer and the output type will be an integer.
	/// </remarks>
	internal class OperationOutput
	{
		private OperationOutput.OutputType dataType;

		private string opType;

		private string measurementType;

		private object value;

		private const string TypeSep = ":";

		private const string MeasurementSep = "*";

		private const string StringSep = ";";

		internal enum OutputType
		{
			String,
			Float,
			Long,
			Double,
			Integer
		}

		/// <summary>
		/// Parses a given key according to the expected key format and forms the given
		/// segments.
		/// </summary>
		/// <param name="key">the key in expected dataType:operationType*measurementType format
		/// 	</param>
		/// <param name="value">a generic value expected to match the output type</param>
		/// <exception cref="System.ArgumentException">if invalid format</exception>
		internal OperationOutput(string key, object value)
		{
			int place = key.IndexOf(TypeSep);
			if (place == -1)
			{
				throw new ArgumentException("Invalid key format - no type seperator - " + TypeSep
					);
			}
			try
			{
				dataType = OperationOutput.OutputType.ValueOf(StringUtils.ToUpperCase(Sharpen.Runtime.Substring
					(key, 0, place)));
			}
			catch (Exception e)
			{
				throw new ArgumentException("Invalid key format - invalid output type", e);
			}
			key = Sharpen.Runtime.Substring(key, place + 1);
			place = key.IndexOf(MeasurementSep);
			if (place == -1)
			{
				throw new ArgumentException("Invalid key format - no measurement seperator - " + 
					MeasurementSep);
			}
			opType = Sharpen.Runtime.Substring(key, 0, place);
			measurementType = Sharpen.Runtime.Substring(key, place + 1);
			this.value = value;
		}

		internal OperationOutput(Text key, object value)
			: this(key.ToString(), value)
		{
		}

		public override string ToString()
		{
			return GetKeyString() + " (" + this.value + ")";
		}

		internal OperationOutput(OperationOutput.OutputType dataType, string opType, string
			 measurementType, object value)
		{
			this.dataType = dataType;
			this.opType = opType;
			this.measurementType = measurementType;
			this.value = value;
		}

		/// <summary>Merges according to the documented rules for merging.</summary>
		/// <remarks>
		/// Merges according to the documented rules for merging. Only will merge if
		/// measurement type and operation type is the same.
		/// </remarks>
		/// <param name="o1">the first object to merge with the second</param>
		/// <param name="o2">the second object.</param>
		/// <returns>OperationOutput merged output.</returns>
		/// <exception cref="System.ArgumentException">if unable to merge due to incompatible formats/types
		/// 	</exception>
		internal static Org.Apache.Hadoop.FS.Slive.OperationOutput Merge(Org.Apache.Hadoop.FS.Slive.OperationOutput
			 o1, Org.Apache.Hadoop.FS.Slive.OperationOutput o2)
		{
			if (o1.GetMeasurementType().Equals(o2.GetMeasurementType()) && o1.GetOperationType
				().Equals(o2.GetOperationType()))
			{
				object newvalue = null;
				OperationOutput.OutputType newtype = null;
				string opType = o1.GetOperationType();
				string mType = o1.GetMeasurementType();
				if (o1.GetOutputType() == OperationOutput.OutputType.String || o2.GetOutputType()
					 == OperationOutput.OutputType.String)
				{
					newtype = OperationOutput.OutputType.String;
					StringBuilder str = new StringBuilder();
					str.Append(o1.GetValue());
					str.Append(StringSep);
					str.Append(o2.GetValue());
					newvalue = str.ToString();
				}
				else
				{
					if (o1.GetOutputType() == OperationOutput.OutputType.Double || o2.GetOutputType()
						 == OperationOutput.OutputType.Double)
					{
						newtype = OperationOutput.OutputType.Double;
						try
						{
							newvalue = double.ParseDouble(o1.GetValue().ToString()) + double.ParseDouble(o2.GetValue
								().ToString());
						}
						catch (FormatException e)
						{
							throw new ArgumentException("Unable to combine a type with a double " + o1 + " & "
								 + o2, e);
						}
					}
					else
					{
						if (o1.GetOutputType() == OperationOutput.OutputType.Float || o2.GetOutputType() 
							== OperationOutput.OutputType.Float)
						{
							newtype = OperationOutput.OutputType.Float;
							try
							{
								newvalue = float.ParseFloat(o1.GetValue().ToString()) + float.ParseFloat(o2.GetValue
									().ToString());
							}
							catch (FormatException e)
							{
								throw new ArgumentException("Unable to combine a type with a float " + o1 + " & "
									 + o2, e);
							}
						}
						else
						{
							if (o1.GetOutputType() == OperationOutput.OutputType.Long || o2.GetOutputType() ==
								 OperationOutput.OutputType.Long)
							{
								newtype = OperationOutput.OutputType.Long;
								try
								{
									newvalue = long.Parse(o1.GetValue().ToString()) + long.Parse(o2.GetValue().ToString
										());
								}
								catch (FormatException e)
								{
									throw new ArgumentException("Unable to combine a type with a long " + o1 + " & " 
										+ o2, e);
								}
							}
							else
							{
								if (o1.GetOutputType() == OperationOutput.OutputType.Integer || o2.GetOutputType(
									) == OperationOutput.OutputType.Integer)
								{
									newtype = OperationOutput.OutputType.Integer;
									try
									{
										newvalue = System.Convert.ToInt32(o1.GetValue().ToString()) + System.Convert.ToInt32
											(o2.GetValue().ToString());
									}
									catch (FormatException e)
									{
										throw new ArgumentException("Unable to combine a type with an int " + o1 + " & " 
											+ o2, e);
									}
								}
							}
						}
					}
				}
				return new Org.Apache.Hadoop.FS.Slive.OperationOutput(newtype, opType, mType, newvalue
					);
			}
			else
			{
				throw new ArgumentException("Unable to combine dissimilar types " + o1 + " & " + 
					o2);
			}
		}

		/// <summary>Formats the key for output</summary>
		/// <returns>String</returns>
		private string GetKeyString()
		{
			StringBuilder str = new StringBuilder();
			str.Append(GetOutputType().ToString());
			str.Append(TypeSep);
			str.Append(GetOperationType());
			str.Append(MeasurementSep);
			str.Append(GetMeasurementType());
			return str.ToString();
		}

		/// <summary>Retrieves the key in a hadoop text object</summary>
		/// <returns>Text text output</returns>
		internal virtual Org.Apache.Hadoop.IO.Text GetKey()
		{
			return new Org.Apache.Hadoop.IO.Text(GetKeyString());
		}

		/// <summary>Gets the output value in text format</summary>
		/// <returns>Text</returns>
		internal virtual Org.Apache.Hadoop.IO.Text GetOutputValue()
		{
			StringBuilder valueStr = new StringBuilder();
			valueStr.Append(GetValue());
			return new Org.Apache.Hadoop.IO.Text(valueStr.ToString());
		}

		/// <summary>
		/// Gets the object that represents this value (expected to match the output
		/// data type)
		/// </summary>
		/// <returns>Object</returns>
		internal virtual object GetValue()
		{
			return value;
		}

		/// <summary>Gets the output data type of this class.</summary>
		internal virtual OperationOutput.OutputType GetOutputType()
		{
			return dataType;
		}

		/// <summary>Gets the operation type this object represents.</summary>
		/// <returns>String</returns>
		internal virtual string GetOperationType()
		{
			return opType;
		}

		/// <summary>Gets the measurement type this object represents.</summary>
		/// <returns>String</returns>
		internal virtual string GetMeasurementType()
		{
			return measurementType;
		}
	}
}
