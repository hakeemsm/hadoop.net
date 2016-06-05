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
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	public class MutableMetricsFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MutableMetricsFactory)
			);

		internal virtual MutableMetric NewForField(FieldInfo field, Metric annotation, MetricsRegistry
			 registry)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("field " + field + " with annotation " + annotation);
			}
			MetricsInfo info = GetInfo(annotation, field);
			MutableMetric metric = NewForField(field, annotation);
			if (metric != null)
			{
				registry.Add(info.Name(), metric);
				return metric;
			}
			Type cls = field.FieldType;
			if (cls == typeof(MutableCounterInt))
			{
				return registry.NewCounter(info, 0);
			}
			if (cls == typeof(MutableCounterLong))
			{
				return registry.NewCounter(info, 0L);
			}
			if (cls == typeof(MutableGaugeInt))
			{
				return registry.NewGauge(info, 0);
			}
			if (cls == typeof(MutableGaugeLong))
			{
				return registry.NewGauge(info, 0L);
			}
			if (cls == typeof(MutableRate))
			{
				return registry.NewRate(info.Name(), info.Description(), annotation.Always());
			}
			if (cls == typeof(MutableRates))
			{
				return new MutableRates(registry);
			}
			if (cls == typeof(MutableStat))
			{
				return registry.NewStat(info.Name(), info.Description(), annotation.SampleName(), 
					annotation.ValueName(), annotation.Always());
			}
			throw new MetricsException("Unsupported metric field " + field.Name + " of type "
				 + field.FieldType.FullName);
		}

		internal virtual MutableMetric NewForMethod(object source, MethodInfo method, Metric
			 annotation, MetricsRegistry registry)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("method " + method + " with annotation " + annotation);
			}
			MetricsInfo info = GetInfo(annotation, method);
			MutableMetric metric = NewForMethod(source, method, annotation);
			metric = metric != null ? metric : new MethodMetric(source, method, info, annotation
				.Type());
			registry.Add(info.Name(), metric);
			return metric;
		}

		/// <summary>Override to handle custom mutable metrics for fields</summary>
		/// <param name="field">of the metric</param>
		/// <param name="annotation">of the field</param>
		/// <returns>a new metric object or null</returns>
		protected internal virtual MutableMetric NewForField(FieldInfo field, Metric annotation
			)
		{
			return null;
		}

		/// <summary>Override to handle custom mutable metrics for methods</summary>
		/// <param name="source">the metrics source object</param>
		/// <param name="method">to return the metric</param>
		/// <param name="annotation">of the method</param>
		/// <returns>a new metric object or null</returns>
		protected internal virtual MutableMetric NewForMethod(object source, MethodInfo method
			, Metric annotation)
		{
			return null;
		}

		protected internal virtual MetricsInfo GetInfo(Metric annotation, FieldInfo field
			)
		{
			return GetInfo(annotation, GetName(field));
		}

		protected internal virtual string GetName(FieldInfo field)
		{
			return StringUtils.Capitalize(field.Name);
		}

		protected internal virtual MetricsInfo GetInfo(Metric annotation, MethodInfo method
			)
		{
			return GetInfo(annotation, GetName(method));
		}

		protected internal virtual MetricsInfo GetInfo(Type cls, Metrics annotation)
		{
			string name = annotation.Name();
			string about = annotation.About();
			string name2 = name.IsEmpty() ? cls.Name : name;
			return Interns.Info(name2, about.IsEmpty() ? name2 : about);
		}

		protected internal virtual string GetName(MethodInfo method)
		{
			string methodName = method.Name;
			if (methodName.StartsWith("get"))
			{
				return StringUtils.Capitalize(Runtime.Substring(methodName, 3));
			}
			return StringUtils.Capitalize(methodName);
		}

		protected internal virtual MetricsInfo GetInfo(Metric annotation, string defaultName
			)
		{
			string[] value = annotation.Value();
			if (value.Length == 2)
			{
				return Interns.Info(value[0], value[1]);
			}
			if (value.Length == 1)
			{
				return Interns.Info(defaultName, value[0]);
			}
			return Interns.Info(defaultName, defaultName);
		}
	}
}
