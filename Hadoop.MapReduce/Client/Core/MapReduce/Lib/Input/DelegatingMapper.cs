using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// that delegates behavior of paths to multiple other
	/// mappers.
	/// </summary>
	/// <seealso cref="MultipleInputs#addInputPath(Job,Path,Class,Class)"/>
	public class DelegatingMapper<K1, V1, K2, V2> : Mapper<K1, V1, K2, V2>
	{
		private Mapper<K1, V1, K2, V2> mapper;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Setup(Mapper.Context context)
		{
			// Find the Mapper from the TaggedInputSplit.
			TaggedInputSplit inputSplit = (TaggedInputSplit)context.GetInputSplit();
			mapper = (Mapper<K1, V1, K2, V2>)ReflectionUtils.NewInstance(inputSplit.GetMapperClass
				(), context.GetConfiguration());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Run(Mapper.Context context)
		{
			Setup(context);
			mapper.Run(context);
			Cleanup(context);
		}
	}
}
