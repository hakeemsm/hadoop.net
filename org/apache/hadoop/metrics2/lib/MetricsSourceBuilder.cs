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
using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>Helper class to build metrics source object from annotations</summary>
	public class MetricsSourceBuilder
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MetricsSourceBuilder
			)));

		private readonly object source;

		private readonly org.apache.hadoop.metrics2.lib.MutableMetricsFactory factory;

		private readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry;

		private org.apache.hadoop.metrics2.MetricsInfo info;

		private bool hasAtMetric = false;

		private bool hasRegistry = false;

		internal MetricsSourceBuilder(object source, org.apache.hadoop.metrics2.lib.MutableMetricsFactory
			 factory)
		{
			this.source = com.google.common.@base.Preconditions.checkNotNull(source, "source"
				);
			this.factory = com.google.common.@base.Preconditions.checkNotNull(factory, "mutable metrics factory"
				);
			java.lang.Class cls = Sharpen.Runtime.getClassForObject(source);
			registry = initRegistry(source);
			foreach (java.lang.reflect.Field field in org.apache.hadoop.util.ReflectionUtils.
				getDeclaredFieldsIncludingInherited(cls))
			{
				add(source, field);
			}
			foreach (java.lang.reflect.Method method in org.apache.hadoop.util.ReflectionUtils
				.getDeclaredMethodsIncludingInherited(cls))
			{
				add(source, method);
			}
		}

		public virtual org.apache.hadoop.metrics2.MetricsSource build()
		{
			if (source is org.apache.hadoop.metrics2.MetricsSource)
			{
				if (hasAtMetric && !hasRegistry)
				{
					throw new org.apache.hadoop.metrics2.MetricsException("Hybrid metrics: registry required."
						);
				}
				return (org.apache.hadoop.metrics2.MetricsSource)source;
			}
			else
			{
				if (!hasAtMetric)
				{
					throw new org.apache.hadoop.metrics2.MetricsException("No valid @Metric annotation found."
						);
				}
			}
			return new _MetricsSource_76(this);
		}

		private sealed class _MetricsSource_76 : org.apache.hadoop.metrics2.MetricsSource
		{
			public _MetricsSource_76(MetricsSourceBuilder _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void getMetrics(org.apache.hadoop.metrics2.MetricsCollector builder, bool 
				all)
			{
				this._enclosing.registry.snapshot(builder.addRecord(this._enclosing.registry.info
					()), all);
			}

			private readonly MetricsSourceBuilder _enclosing;
		}

		public virtual org.apache.hadoop.metrics2.MetricsInfo info()
		{
			return info;
		}

		private org.apache.hadoop.metrics2.lib.MetricsRegistry initRegistry(object source
			)
		{
			java.lang.Class cls = Sharpen.Runtime.getClassForObject(source);
			org.apache.hadoop.metrics2.lib.MetricsRegistry r = null;
			// Get the registry if it already exists.
			foreach (java.lang.reflect.Field field in org.apache.hadoop.util.ReflectionUtils.
				getDeclaredFieldsIncludingInherited(cls))
			{
				if (field.getType() != Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MetricsRegistry
					)))
				{
					continue;
				}
				try
				{
					field.setAccessible(true);
					r = (org.apache.hadoop.metrics2.lib.MetricsRegistry)field.get(source);
					hasRegistry = r != null;
					break;
				}
				catch (System.Exception e)
				{
					LOG.warn("Error accessing field " + field, e);
					continue;
				}
			}
			// Create a new registry according to annotation
			foreach (java.lang.annotation.Annotation annotation in cls.getAnnotations())
			{
				if (annotation is org.apache.hadoop.metrics2.annotation.Metrics)
				{
					org.apache.hadoop.metrics2.annotation.Metrics ma = (org.apache.hadoop.metrics2.annotation.Metrics
						)annotation;
					info = factory.getInfo(cls, ma);
					if (r == null)
					{
						r = new org.apache.hadoop.metrics2.lib.MetricsRegistry(info);
					}
					r.setContext(ma.context());
				}
			}
			if (r == null)
			{
				return new org.apache.hadoop.metrics2.lib.MetricsRegistry(cls.getSimpleName());
			}
			return r;
		}

		private void add(object source, java.lang.reflect.Field field)
		{
			foreach (java.lang.annotation.Annotation annotation in field.getAnnotations())
			{
				if (!(annotation is org.apache.hadoop.metrics2.annotation.Metric))
				{
					continue;
				}
				try
				{
					// skip fields already set
					field.setAccessible(true);
					if (field.get(source) != null)
					{
						continue;
					}
				}
				catch (System.Exception e)
				{
					LOG.warn("Error accessing field " + field + " annotated with" + annotation, e);
					continue;
				}
				org.apache.hadoop.metrics2.lib.MutableMetric mutable = factory.newForField(field, 
					(org.apache.hadoop.metrics2.annotation.Metric)annotation, registry);
				if (mutable != null)
				{
					try
					{
						field.set(source, mutable);
						hasAtMetric = true;
					}
					catch (System.Exception e)
					{
						throw new org.apache.hadoop.metrics2.MetricsException("Error setting field " + field
							 + " annotated with " + annotation, e);
					}
				}
			}
		}

		private void add(object source, java.lang.reflect.Method method)
		{
			foreach (java.lang.annotation.Annotation annotation in method.getAnnotations())
			{
				if (!(annotation is org.apache.hadoop.metrics2.annotation.Metric))
				{
					continue;
				}
				factory.newForMethod(source, method, (org.apache.hadoop.metrics2.annotation.Metric
					)annotation, registry);
				hasAtMetric = true;
			}
		}
	}
}
