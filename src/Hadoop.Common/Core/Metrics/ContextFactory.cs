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
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Metrics.Spi;


namespace Org.Apache.Hadoop.Metrics
{
	/// <summary>Factory class for creating MetricsContext objects.</summary>
	/// <remarks>
	/// Factory class for creating MetricsContext objects.  To obtain an instance
	/// of this class, use the static <code>getFactory()</code> method.
	/// </remarks>
	public class ContextFactory
	{
		private const string PropertiesFile = "/hadoop-metrics.properties";

		private const string ContextClassSuffix = ".class";

		private const string DefaultContextClassname = "org.apache.hadoop.metrics.spi.NullContext";

		private static Org.Apache.Hadoop.Metrics.ContextFactory theFactory = null;

		private IDictionary<string, object> attributeMap = new Dictionary<string, object>
			();

		private IDictionary<string, MetricsContext> contextMap = new Dictionary<string, MetricsContext
			>();

		private static IDictionary<string, MetricsContext> nullContextMap = new Dictionary
			<string, MetricsContext>();

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
		public virtual object GetAttribute(string attributeName)
		{
			return attributeMap[attributeName];
		}

		/// <summary>Returns the names of all the factory's attributes.</summary>
		/// <returns>the attribute names</returns>
		public virtual string[] GetAttributeNames()
		{
			string[] result = new string[attributeMap.Count];
			int i = 0;
			// for (String attributeName : attributeMap.keySet()) {
			IEnumerator it = attributeMap.Keys.GetEnumerator();
			while (it.HasNext())
			{
				result[i++] = (string)it.Next();
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
		public virtual void SetAttribute(string attributeName, object value)
		{
			attributeMap[attributeName] = value;
		}

		/// <summary>Removes the named attribute if it exists.</summary>
		/// <param name="attributeName">the attribute name</param>
		public virtual void RemoveAttribute(string attributeName)
		{
			Collections.Remove(attributeMap, attributeName);
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
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		public virtual MetricsContext GetContext(string refName, string contextName)
		{
			lock (this)
			{
				MetricsContext metricsContext = contextMap[refName];
				if (metricsContext == null)
				{
					string classNameAttribute = refName + ContextClassSuffix;
					string className = (string)GetAttribute(classNameAttribute);
					if (className == null)
					{
						className = DefaultContextClassname;
					}
					Type contextClass = Runtime.GetType(className);
					metricsContext = (MetricsContext)System.Activator.CreateInstance(contextClass);
					metricsContext.Init(contextName, this);
					contextMap[contextName] = metricsContext;
				}
				return metricsContext;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		public virtual MetricsContext GetContext(string contextName)
		{
			lock (this)
			{
				return GetContext(contextName, contextName);
			}
		}

		/// <summary>Returns all MetricsContexts built by this factory.</summary>
		public virtual ICollection<MetricsContext> GetAllContexts()
		{
			lock (this)
			{
				// Make a copy to avoid race conditions with creating new contexts.
				return new AList<MetricsContext>(contextMap.Values);
			}
		}

		/// <summary>Returns a "null" context - one which does nothing.</summary>
		public static MetricsContext GetNullContext(string contextName)
		{
			lock (typeof(ContextFactory))
			{
				MetricsContext nullContext = nullContextMap[contextName];
				if (nullContext == null)
				{
					nullContext = new NullContext();
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
		public static Org.Apache.Hadoop.Metrics.ContextFactory GetFactory()
		{
			lock (typeof(ContextFactory))
			{
				if (theFactory == null)
				{
					theFactory = new Org.Apache.Hadoop.Metrics.ContextFactory();
					theFactory.SetAttributes();
				}
				return theFactory;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SetAttributes()
		{
			InputStream @is = GetType().GetResourceAsStream(PropertiesFile);
			if (@is != null)
			{
				try
				{
					Properties properties = new Properties();
					properties.Load(@is);
					//for (Object propertyNameObj : properties.keySet()) {
					IEnumerator it = properties.Keys.GetEnumerator();
					while (it.HasNext())
					{
						string propertyName = (string)it.Next();
						string propertyValue = properties.GetProperty(propertyName);
						SetAttribute(propertyName, propertyValue);
					}
				}
				finally
				{
					@is.Close();
				}
			}
		}
	}
}
