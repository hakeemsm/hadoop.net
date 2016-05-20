/*
* ContextFactory.java
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.metrics
{
	/// <summary>Factory class for creating MetricsContext objects.</summary>
	/// <remarks>
	/// Factory class for creating MetricsContext objects.  To obtain an instance
	/// of this class, use the static <code>getFactory()</code> method.
	/// </remarks>
	public class ContextFactory
	{
		private const string PROPERTIES_FILE = "/hadoop-metrics.properties";

		private const string CONTEXT_CLASS_SUFFIX = ".class";

		private const string DEFAULT_CONTEXT_CLASSNAME = "org.apache.hadoop.metrics.spi.NullContext";

		private static org.apache.hadoop.metrics.ContextFactory theFactory = null;

		private System.Collections.Generic.IDictionary<string, object> attributeMap = new 
			System.Collections.Generic.Dictionary<string, object>();

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics.MetricsContext
			> contextMap = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.metrics.MetricsContext
			>();

		private static System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics.MetricsContext
			> nullContextMap = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.metrics.MetricsContext
			>();

		/// <summary>Creates a new instance of ContextFactory</summary>
		protected internal ContextFactory()
		{
		}

		// Used only when contexts, or the ContextFactory itself, cannot be
		// created.
		/// <summary>
		/// Returns the value of the named attribute, or null if there is no
		/// attribute of that name.
		/// </summary>
		/// <param name="attributeName">the attribute name</param>
		/// <returns>the attribute value</returns>
		public virtual object getAttribute(string attributeName)
		{
			return attributeMap[attributeName];
		}

		/// <summary>Returns the names of all the factory's attributes.</summary>
		/// <returns>the attribute names</returns>
		public virtual string[] getAttributeNames()
		{
			string[] result = new string[attributeMap.Count];
			int i = 0;
			// for (String attributeName : attributeMap.keySet()) {
			System.Collections.IEnumerator it = attributeMap.Keys.GetEnumerator();
			while (it.MoveNext())
			{
				result[i++] = (string)it.Current;
			}
			return result;
		}

		/// <summary>
		/// Sets the named factory attribute to the specified value, creating it
		/// if it did not already exist.
		/// </summary>
		/// <remarks>
		/// Sets the named factory attribute to the specified value, creating it
		/// if it did not already exist.  If the value is null, this is the same as
		/// calling removeAttribute.
		/// </remarks>
		/// <param name="attributeName">the attribute name</param>
		/// <param name="value">the new attribute value</param>
		public virtual void setAttribute(string attributeName, object value)
		{
			attributeMap[attributeName] = value;
		}

		/// <summary>Removes the named attribute if it exists.</summary>
		/// <param name="attributeName">the attribute name</param>
		public virtual void removeAttribute(string attributeName)
		{
			Sharpen.Collections.Remove(attributeMap, attributeName);
		}

		/// <summary>
		/// Returns the named MetricsContext instance, constructing it if necessary
		/// using the factory's current configuration attributes.
		/// </summary>
		/// <remarks>
		/// Returns the named MetricsContext instance, constructing it if necessary
		/// using the factory's current configuration attributes. <p/>
		/// When constructing the instance, if the factory property
		/// <i>contextName</i>.class</code> exists,
		/// its value is taken to be the name of the class to instantiate.  Otherwise,
		/// the default is to create an instance of
		/// <code>org.apache.hadoop.metrics.spi.NullContext</code>, which is a
		/// dummy "no-op" context which will cause all metric data to be discarded.
		/// </remarks>
		/// <param name="contextName">the name of the context</param>
		/// <returns>the named MetricsContext</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		public virtual org.apache.hadoop.metrics.MetricsContext getContext(string refName
			, string contextName)
		{
			lock (this)
			{
				org.apache.hadoop.metrics.MetricsContext metricsContext = contextMap[refName];
				if (metricsContext == null)
				{
					string classNameAttribute = refName + CONTEXT_CLASS_SUFFIX;
					string className = (string)getAttribute(classNameAttribute);
					if (className == null)
					{
						className = DEFAULT_CONTEXT_CLASSNAME;
					}
					java.lang.Class contextClass = java.lang.Class.forName(className);
					metricsContext = (org.apache.hadoop.metrics.MetricsContext)contextClass.newInstance
						();
					metricsContext.init(contextName, this);
					contextMap[contextName] = metricsContext;
				}
				return metricsContext;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		public virtual org.apache.hadoop.metrics.MetricsContext getContext(string contextName
			)
		{
			lock (this)
			{
				return getContext(contextName, contextName);
			}
		}

		/// <summary>Returns all MetricsContexts built by this factory.</summary>
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.metrics.MetricsContext
			> getAllContexts()
		{
			lock (this)
			{
				// Make a copy to avoid race conditions with creating new contexts.
				return new System.Collections.Generic.List<org.apache.hadoop.metrics.MetricsContext
					>(contextMap.Values);
			}
		}

		/// <summary>Returns a "null" context - one which does nothing.</summary>
		public static org.apache.hadoop.metrics.MetricsContext getNullContext(string contextName
			)
		{
			lock (typeof(ContextFactory))
			{
				org.apache.hadoop.metrics.MetricsContext nullContext = nullContextMap[contextName
					];
				if (nullContext == null)
				{
					nullContext = new org.apache.hadoop.metrics.spi.NullContext();
					nullContextMap[contextName] = nullContext;
				}
				return nullContext;
			}
		}

		/// <summary>
		/// Returns the singleton ContextFactory instance, constructing it if
		/// necessary.
		/// </summary>
		/// <remarks>
		/// Returns the singleton ContextFactory instance, constructing it if
		/// necessary. <p/>
		/// When the instance is constructed, this method checks if the file
		/// <code>hadoop-metrics.properties</code> exists on the class path.  If it
		/// exists, it must be in the format defined by java.util.Properties, and all
		/// the properties in the file are set as attributes on the newly created
		/// ContextFactory instance.
		/// </remarks>
		/// <returns>the singleton ContextFactory instance</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.metrics.ContextFactory getFactory()
		{
			lock (typeof(ContextFactory))
			{
				if (theFactory == null)
				{
					theFactory = new org.apache.hadoop.metrics.ContextFactory();
					theFactory.setAttributes();
				}
				return theFactory;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void setAttributes()
		{
			java.io.InputStream @is = Sharpen.Runtime.getClassForObject(this).getResourceAsStream
				(PROPERTIES_FILE);
			if (@is != null)
			{
				try
				{
					java.util.Properties properties = new java.util.Properties();
					properties.load(@is);
					//for (Object propertyNameObj : properties.keySet()) {
					System.Collections.IEnumerator it = properties.Keys.GetEnumerator();
					while (it.MoveNext())
					{
						string propertyName = (string)it.Current;
						string propertyValue = properties.getProperty(propertyName);
						setAttribute(propertyName, propertyValue);
					}
				}
				finally
				{
					@is.close();
				}
			}
		}
	}
}
