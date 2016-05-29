using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	/// <summary>Interface defining events api.</summary>
	public interface Event<Type>
		where Type : Enum<TYPE>
	{
		TYPE GetType();

		long GetTimestamp();

		string ToString();
	}
}
