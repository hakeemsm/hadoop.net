using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Factories
{
	public interface RecordFactory
	{
		T NewRecordInstance<T>();
	}
}
