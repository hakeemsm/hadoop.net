using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This abstract class implements some common functionalities of the
	/// the generic mapper, reducer and combiner classes of Aggregate.
	/// </summary>
	public class ValueAggregatorJobBase<K1, V1>
		where K1 : WritableComparable<object>
		where V1 : Writable
	{
		public const string Descriptor = "mapreduce.aggregate.descriptor";

		public const string DescriptorNum = "mapreduce.aggregate.descriptor.num";

		public const string UserJar = "mapreduce.aggregate.user.jar.file";

		protected internal static AList<ValueAggregatorDescriptor> aggregatorDescriptorList
			 = null;

		public static void Setup(Configuration job)
		{
			InitializeMySpec(job);
			LogSpec();
		}

		protected internal static ValueAggregatorDescriptor GetValueAggregatorDescriptor(
			string spec, Configuration conf)
		{
			if (spec == null)
			{
				return null;
			}
			string[] segments = spec.Split(",", -1);
			string type = segments[0];
			if (type.CompareToIgnoreCase("UserDefined") == 0)
			{
				string className = segments[1];
				return new UserDefinedValueAggregatorDescriptor(className, conf);
			}
			return null;
		}

		protected internal static AList<ValueAggregatorDescriptor> GetAggregatorDescriptors
			(Configuration conf)
		{
			int num = conf.GetInt(DescriptorNum, 0);
			AList<ValueAggregatorDescriptor> retv = new AList<ValueAggregatorDescriptor>(num);
			for (int i = 0; i < num; i++)
			{
				string spec = conf.Get(Descriptor + "." + i);
				ValueAggregatorDescriptor ad = GetValueAggregatorDescriptor(spec, conf);
				if (ad != null)
				{
					retv.AddItem(ad);
				}
			}
			return retv;
		}

		private static void InitializeMySpec(Configuration conf)
		{
			aggregatorDescriptorList = GetAggregatorDescriptors(conf);
			if (aggregatorDescriptorList.Count == 0)
			{
				aggregatorDescriptorList.AddItem(new UserDefinedValueAggregatorDescriptor(typeof(
					ValueAggregatorBaseDescriptor).GetCanonicalName(), conf));
			}
		}

		protected internal static void LogSpec()
		{
		}
	}
}
