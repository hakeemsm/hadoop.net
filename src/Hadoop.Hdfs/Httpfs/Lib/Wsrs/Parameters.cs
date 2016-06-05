using System.Collections.Generic;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	/// <summary>Class that contains all parsed JAX-RS parameters.</summary>
	/// <remarks>
	/// Class that contains all parsed JAX-RS parameters.
	/// <p>
	/// Instances are created by the
	/// <see cref="ParametersProvider"/>
	/// class.
	/// </remarks>
	public class Parameters
	{
		private IDictionary<string, IList<Param<object>>> @params;

		/// <summary>Constructor that receives the request parsed parameters.</summary>
		/// <param name="params">the request parsed parameters.</param>
		public Parameters(IDictionary<string, IList<Param<object>>> @params)
		{
			this.@params = @params;
		}

		/// <summary>Returns the value of a request parsed parameter.</summary>
		/// <param name="name">parameter name.</param>
		/// <param name="klass">class of the parameter, used for value casting.</param>
		/// <returns>the value of the parameter.</returns>
		public virtual V Get<V, T>(string name)
			where T : Param<V>
		{
			System.Type klass = typeof(T);
			IList<Param<object>> multiParams = (IList<Param<object>>)@params[name];
			if (multiParams != null && multiParams.Count > 0)
			{
				return ((T)multiParams[0]).Value();
			}
			// Return first value;
			return null;
		}

		/// <summary>Returns the values of a request parsed parameter.</summary>
		/// <param name="name">parameter name.</param>
		/// <param name="klass">class of the parameter, used for value casting.</param>
		/// <returns>the values of the parameter.</returns>
		public virtual IList<V> GetValues<V, T>(string name)
			where T : Param<V>
		{
			System.Type klass = typeof(T);
			IList<Param<object>> multiParams = (IList<Param<object>>)@params[name];
			IList<V> values = Lists.NewArrayList();
			if (multiParams != null)
			{
				foreach (Param<object> param in multiParams)
				{
					V value = ((T)param).Value();
					if (value != null)
					{
						values.AddItem(value);
					}
				}
			}
			return values;
		}
	}
}
