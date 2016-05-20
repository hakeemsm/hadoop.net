using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>A container class for javax.servlet.Filter.</summary>
	public interface FilterContainer
	{
		/// <summary>Add a filter to the container.</summary>
		/// <param name="name">Filter name</param>
		/// <param name="classname">Filter class name</param>
		/// <param name="parameters">a map from parameter names to initial values</param>
		void addFilter(string name, string classname, System.Collections.Generic.IDictionary
			<string, string> parameters);

		/// <summary>Add a global filter to the container.</summary>
		/// <param name="name">filter name</param>
		/// <param name="classname">filter class name</param>
		/// <param name="parameters">a map from parameter names to initial values</param>
		void addGlobalFilter(string name, string classname, System.Collections.Generic.IDictionary
			<string, string> parameters);
	}
}
