/*
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
using System.Reflection;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Helper class to build metrics source object from annotations</summary>
	public class MetricsSourceBuilder
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Lib.MetricsSourceBuilder
			));

		private readonly object source;

		private readonly MutableMetricsFactory factory;

		private readonly MetricsRegistry registry;

		private MetricsInfo info;

		private bool hasAtMetric = false;

		private bool hasRegistry = false;

		internal MetricsSourceBuilder(object source, MutableMetricsFactory factory)
		{
			this.source = Preconditions.CheckNotNull(source, "source");
			this.factory = Preconditions.CheckNotNull(factory, "mutable metrics factory");
			Type cls = source.GetType();
			registry = InitRegistry(source);
			foreach (FieldInfo field in ReflectionUtils.GetDeclaredFieldsIncludingInherited(cls
				))
			{
				Add(source, field);
			}
			foreach (MethodInfo method in ReflectionUtils.GetDeclaredMethodsIncludingInherited
				(cls))
			{
				Add(source, method);
			}
		}

		public virtual MetricsSource Build()
		{
			if (source is MetricsSource)
			{
				if (hasAtMetric && !hasRegistry)
				{
					throw new MetricsException("Hybrid metrics: registry required.");
				}
				return (MetricsSource)source;
			}
			else
			{
				if (!hasAtMetric)
				{
					throw new MetricsException("No valid @Metric annotation found.");
				}
			}
			return new _MetricsSource_76(this);
		}

		private sealed class _MetricsSource_76 : MetricsSource
		{
			public _MetricsSource_76(MetricsSourceBuilder _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void GetMetrics(MetricsCollector builder, bool all)
			{
				this._enclosing.registry.Snapshot(builder.AddRecord(this._enclosing.registry.Info
					()), all);
			}

			private readonly MetricsSourceBuilder _enclosing;
		}

		public virtual MetricsInfo Info()
		{
			return info;
		}

		private MetricsRegistry InitRegistry(object source)
		{
			Type cls = source.GetType();
			MetricsRegistry r = null;
			// Get the registry if it already exists.
			foreach (FieldInfo field in ReflectionUtils.GetDeclaredFieldsIncludingInherited(cls
				))
			{
				if (field.FieldType != typeof(MetricsRegistry))
				{
					continue;
				}
				try
				{
					r = (MetricsRegistry)field.GetValue(source);
					hasRegistry = r != null;
					break;
				}
				catch (Exception e)
				{
					Log.Warn("Error accessing field " + field, e);
					continue;
				}
			}
			// Create a new registry according to annotation
			foreach (Sharpen.Annotation.Annotation annotation in cls.GetAnnotations())
			{
				if (annotation is Metrics)
				{
					Metrics ma = (Metrics)annotation;
					info = factory.GetInfo(cls, ma);
					if (r == null)
					{
						r = new MetricsRegistry(info);
					}
					r.SetContext(ma.Context());
				}
			}
			if (r == null)
			{
				return new MetricsRegistry(cls.Name);
			}
			return r;
		}

		private void Add(object source, FieldInfo field)
		{
			foreach (Sharpen.Annotation.Annotation annotation in field.GetAnnotations())
			{
				if (!(annotation is Metric))
				{
					continue;
				}
				try
				{
					// skip fields already set
					if (field.GetValue(source) != null)
					{
						continue;
					}
				}
				catch (Exception e)
				{
					Log.Warn("Error accessing field " + field + " annotated with" + annotation, e);
					continue;
				}
				MutableMetric mutable = factory.NewForField(field, (Metric)annotation, registry);
				if (mutable != null)
				{
					try
					{
						field.SetValue(source, mutable);
						hasAtMetric = true;
					}
					catch (Exception e)
					{
						throw new MetricsException("Error setting field " + field + " annotated with " + 
							annotation, e);
					}
				}
			}
		}

		private void Add(object source, MethodInfo method)
		{
			foreach (Sharpen.Annotation.Annotation annotation in method.GetAnnotations())
			{
				if (!(annotation is Metric))
				{
					continue;
				}
				factory.NewForMethod(source, method, (Metric)annotation, registry);
				hasAtMetric = true;
			}
		}
	}
}
