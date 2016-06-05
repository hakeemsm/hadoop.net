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
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Util;


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Metric generated from a method, mostly used by annotation</summary>
	internal class MethodMetric : MutableMetric
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Lib.MethodMetric
			));

		private readonly object obj;

		private readonly MethodInfo method;

		private readonly MetricsInfo info;

		private readonly MutableMetric impl;

		internal MethodMetric(object obj, MethodInfo method, MetricsInfo info, Metric.Type
			 type)
		{
			this.obj = Preconditions.CheckNotNull(obj, "object");
			this.method = Contracts.CheckArg(method, Runtime.GetParameterTypes(method
				).Length == 0, "Metric method should have no arguments");
			this.info = Preconditions.CheckNotNull(info, "info");
			impl = NewImpl(Preconditions.CheckNotNull(type, "metric type"));
		}

		private MutableMetric NewImpl(Metric.Type metricType)
		{
			Type resType = method.ReturnType;
			switch (metricType)
			{
				case Metric.Type.Counter:
				{
					return NewCounter(resType);
				}

				case Metric.Type.Gauge:
				{
					return NewGauge(resType);
				}

				case Metric.Type.Default:
				{
					return resType == typeof(string) ? NewTag(resType) : NewGauge(resType);
				}

				case Metric.Type.Tag:
				{
					return NewTag(resType);
				}

				default:
				{
					Contracts.CheckArg(metricType, false, "unsupported metric type");
					return null;
				}
			}
		}

		internal virtual MutableMetric NewCounter(Type type)
		{
			if (IsInt(type) || IsLong(type))
			{
				return new _MutableMetric_73(this, type);
			}
			throw new MetricsException("Unsupported counter type: " + type.FullName);
		}

		private sealed class _MutableMetric_73 : MutableMetric
		{
			public _MutableMetric_73(MethodMetric _enclosing, Type type)
			{
				this._enclosing = _enclosing;
				this.type = type;
			}

			public override void Snapshot(MetricsRecordBuilder rb, bool all)
			{
				try
				{
					object ret = this._enclosing.method.Invoke(this._enclosing.obj, (object[])null);
					if (Org.Apache.Hadoop.Metrics2.Lib.MethodMetric.IsInt(type))
					{
						rb.AddCounter(this._enclosing.info, ((int)ret));
					}
					else
					{
						rb.AddCounter(this._enclosing.info, ((long)ret));
					}
				}
				catch (Exception ex)
				{
					Org.Apache.Hadoop.Metrics2.Lib.MethodMetric.Log.Error("Error invoking method " + 
						this._enclosing.method.Name, ex);
				}
			}

			private readonly MethodMetric _enclosing;

			private readonly Type type;
		}

		internal static bool IsInt(Type type)
		{
			bool ret = type == typeof(int) || type == typeof(int);
			return ret;
		}

		internal static bool IsLong(Type type)
		{
			return type == typeof(long) || type == typeof(long);
		}

		internal static bool IsFloat(Type type)
		{
			return type == typeof(float) || type == typeof(float);
		}

		internal static bool IsDouble(Type type)
		{
			return type == typeof(double) || type == typeof(double);
		}

		internal virtual MutableMetric NewGauge(Type t)
		{
			if (IsInt(t) || IsLong(t) || IsFloat(t) || IsDouble(t))
			{
				return new _MutableMetric_108(this, t);
			}
			throw new MetricsException("Unsupported gauge type: " + t.FullName);
		}

		private sealed class _MutableMetric_108 : MutableMetric
		{
			public _MutableMetric_108(MethodMetric _enclosing, Type t)
			{
				this._enclosing = _enclosing;
				this.t = t;
			}

			public override void Snapshot(MetricsRecordBuilder rb, bool all)
			{
				try
				{
					object ret = this._enclosing.method.Invoke(this._enclosing.obj, (object[])null);
					if (Org.Apache.Hadoop.Metrics2.Lib.MethodMetric.IsInt(t))
					{
						rb.AddGauge(this._enclosing.info, ((int)ret));
					}
					else
					{
						if (Org.Apache.Hadoop.Metrics2.Lib.MethodMetric.IsLong(t))
						{
							rb.AddGauge(this._enclosing.info, ((long)ret));
						}
						else
						{
							if (Org.Apache.Hadoop.Metrics2.Lib.MethodMetric.IsFloat(t))
							{
								rb.AddGauge(this._enclosing.info, ((float)ret));
							}
							else
							{
								rb.AddGauge(this._enclosing.info, ((double)ret));
							}
						}
					}
				}
				catch (Exception ex)
				{
					Org.Apache.Hadoop.Metrics2.Lib.MethodMetric.Log.Error("Error invoking method " + 
						this._enclosing.method.Name, ex);
				}
			}

			private readonly MethodMetric _enclosing;

			private readonly Type t;
		}

		internal virtual MutableMetric NewTag(Type resType)
		{
			if (resType == typeof(string))
			{
				return new _MutableMetric_128(this);
			}
			throw new MetricsException("Unsupported tag type: " + resType.FullName);
		}

		private sealed class _MutableMetric_128 : MutableMetric
		{
			public _MutableMetric_128(MethodMetric _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Snapshot(MetricsRecordBuilder rb, bool all)
			{
				try
				{
					object ret = this._enclosing.method.Invoke(this._enclosing.obj, (object[])null);
					rb.Tag(this._enclosing.info, (string)ret);
				}
				catch (Exception ex)
				{
					Org.Apache.Hadoop.Metrics2.Lib.MethodMetric.Log.Error("Error invoking method " + 
						this._enclosing.method.Name, ex);
				}
			}

			private readonly MethodMetric _enclosing;
		}

		public override void Snapshot(MetricsRecordBuilder builder, bool all)
		{
			impl.Snapshot(builder, all);
		}

		internal static MetricsInfo MetricInfo(MethodInfo method)
		{
			return Interns.Info(NameFrom(method), "Metric for " + method.Name);
		}

		internal static string NameFrom(MethodInfo method)
		{
			string methodName = method.Name;
			if (methodName.StartsWith("get"))
			{
				return StringUtils.Capitalize(Runtime.Substring(methodName, 3));
			}
			return StringUtils.Capitalize(methodName);
		}
	}
}
