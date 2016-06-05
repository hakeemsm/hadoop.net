using System.Collections.Generic;
using Org.Apache.Avro;
using Org.Apache.Avro.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class AvroArrayUtils
	{
		private static readonly Schema ArrayInt = Schema.CreateArray(Schema.Create(Schema.Type
			.Int));

		public static IList<int> NullProgressSplitsArray = new GenericData.Array<int>(0, 
			ArrayInt);

		public static IList<int> ToAvro(int[] values)
		{
			IList<int> result = new AList<int>(values.Length);
			for (int i = 0; i < values.Length; ++i)
			{
				result.AddItem(values[i]);
			}
			return result;
		}

		public static int[] FromAvro(IList<int> avro)
		{
			int[] result = new int[(int)avro.Count];
			int i = 0;
			for (IEnumerator<int> iter = avro.GetEnumerator(); iter.HasNext(); ++i)
			{
				result[i] = iter.Next();
			}
			return result;
		}
	}
}
