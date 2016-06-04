using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>
	/// Something whose
	/// <see cref="Configuration"/>
	/// can be changed at run time.
	/// </summary>
	public interface Reconfigurable : Configurable
	{
		/// <summary>Change a configuration property on this object to the value specified.</summary>
		/// <remarks>
		/// Change a configuration property on this object to the value specified.
		/// Change a configuration property on this object to the value specified
		/// and return the previous value that the configuration property was set to
		/// (or null if it was not previously set). If newVal is null, set the property
		/// to its default value;
		/// If the property cannot be changed, throw a
		/// <see cref="ReconfigurationException"/>
		/// .
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		string ReconfigureProperty(string property, string newVal);

		/// <summary>Return whether a given property is changeable at run time.</summary>
		/// <remarks>
		/// Return whether a given property is changeable at run time.
		/// If isPropertyReconfigurable returns true for a property,
		/// then changeConf should not throw an exception when changing
		/// this property.
		/// </remarks>
		bool IsPropertyReconfigurable(string property);

		/// <summary>Return all the properties that can be changed at run time.</summary>
		ICollection<string> GetReconfigurableProperties();
	}
}
