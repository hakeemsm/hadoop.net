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
using System.Collections.Generic;
using System.Reflection;
using Com.Sun.Javadoc;
using Org.Apache.Hadoop.Classification;

using Reflect;

namespace Org.Apache.Hadoop.Classification.Tools
{
	/// <summary>
	/// Process the
	/// <see cref="Com.Sun.Javadoc.RootDoc"/>
	/// by substituting with (nested) proxy objects that
	/// exclude elements with Private or LimitedPrivate annotations.
	/// <p>
	/// Based on code from http://www.sixlegs.com/blog/java/exclude-javadoc-tag.html.
	/// </summary>
	internal class RootDocProcessor
	{
		internal static string stability = StabilityOptions.UnstableOption;

		internal static bool treatUnannotatedClassesAsPrivate = false;

		public static RootDoc Process(RootDoc root)
		{
			return (RootDoc)Process(root, typeof(RootDoc));
		}

		private static object Process(object obj, Type type)
		{
			if (obj == null)
			{
				return null;
			}
			Type cls = obj.GetType();
			if (cls.FullName.StartsWith("com.sun."))
			{
				return GetProxy(obj);
			}
			else
			{
				if (obj is object[])
				{
					Type componentType = type.IsArray ? type.GetElementType() : cls.GetElementType();
					object[] array = (object[])obj;
					object[] newArray = (object[])System.Array.CreateInstance(componentType, array.Length
						);
					for (int i = 0; i < array.Length; ++i)
					{
						newArray[i] = Process(array[i], componentType);
					}
					return newArray;
				}
			}
			return obj;
		}

		private static IDictionary<object, object> proxies = new WeakHashMap<object, object
			>();

		private static object GetProxy(object obj)
		{
			object proxy = proxies[obj];
			if (proxy == null)
			{
				proxy = Proxy.NewProxyInstance(obj.GetType().GetClassLoader(), obj.GetType().GetInterfaces
					(), new RootDocProcessor.ExcludeHandler(obj));
				proxies[obj] = proxy;
			}
			return proxy;
		}

		private class ExcludeHandler : InvocationHandler
		{
			private object target;

			public ExcludeHandler(object target)
			{
				this.target = target;
			}

			/// <exception cref="System.Exception"/>
			public virtual object Invoke(object proxy, MethodInfo method, object[] args)
			{
				string methodName = method.Name;
				if (target is Doc)
				{
					if (methodName.Equals("isIncluded"))
					{
						Doc doc = (Doc)target;
						return !Exclude(doc) && doc.IsIncluded();
					}
					if (target is RootDoc)
					{
						if (methodName.Equals("classes"))
						{
							return Filter(((RootDoc)target).Classes(), typeof(ClassDoc));
						}
						else
						{
							if (methodName.Equals("specifiedClasses"))
							{
								return Filter(((RootDoc)target).SpecifiedClasses(), typeof(ClassDoc));
							}
							else
							{
								if (methodName.Equals("specifiedPackages"))
								{
									return Filter(((RootDoc)target).SpecifiedPackages(), typeof(PackageDoc));
								}
							}
						}
					}
					else
					{
						if (target is ClassDoc)
						{
							if (IsFiltered(args))
							{
								if (methodName.Equals("methods"))
								{
									return Filter(((ClassDoc)target).Methods(true), typeof(MethodDoc));
								}
								else
								{
									if (methodName.Equals("fields"))
									{
										return Filter(((ClassDoc)target).Fields(true), typeof(FieldDoc));
									}
									else
									{
										if (methodName.Equals("innerClasses"))
										{
											return Filter(((ClassDoc)target).InnerClasses(true), typeof(ClassDoc));
										}
										else
										{
											if (methodName.Equals("constructors"))
											{
												return Filter(((ClassDoc)target).Constructors(true), typeof(ConstructorDoc));
											}
										}
									}
								}
							}
						}
						else
						{
							if (target is PackageDoc)
							{
								if (methodName.Equals("allClasses"))
								{
									if (IsFiltered(args))
									{
										return Filter(((PackageDoc)target).AllClasses(true), typeof(ClassDoc));
									}
									else
									{
										return Filter(((PackageDoc)target).AllClasses(), typeof(ClassDoc));
									}
								}
								else
								{
									if (methodName.Equals("annotationTypes"))
									{
										return Filter(((PackageDoc)target).AnnotationTypes(), typeof(AnnotationTypeDoc));
									}
									else
									{
										if (methodName.Equals("enums"))
										{
											return Filter(((PackageDoc)target).Enums(), typeof(ClassDoc));
										}
										else
										{
											if (methodName.Equals("errors"))
											{
												return Filter(((PackageDoc)target).Errors(), typeof(ClassDoc));
											}
											else
											{
												if (methodName.Equals("exceptions"))
												{
													return Filter(((PackageDoc)target).Exceptions(), typeof(ClassDoc));
												}
												else
												{
													if (methodName.Equals("interfaces"))
													{
														return Filter(((PackageDoc)target).Interfaces(), typeof(ClassDoc));
													}
													else
													{
														if (methodName.Equals("ordinaryClasses"))
														{
															return Filter(((PackageDoc)target).OrdinaryClasses(), typeof(ClassDoc));
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				if (args != null)
				{
					if (methodName.Equals("compareTo") || methodName.Equals("equals") || methodName.Equals
						("overrides") || methodName.Equals("subclassOf"))
					{
						args[0] = Unwrap(args[0]);
					}
				}
				try
				{
					return Process(method.Invoke(target, args), method.ReturnType);
				}
				catch (TargetInvocationException e)
				{
					throw e.InnerException;
				}
			}

			private static bool Exclude(Doc doc)
			{
				AnnotationDesc[] annotations = null;
				if (doc is ProgramElementDoc)
				{
					annotations = ((ProgramElementDoc)doc).Annotations();
				}
				else
				{
					if (doc is PackageDoc)
					{
						annotations = ((PackageDoc)doc).Annotations();
					}
				}
				if (annotations != null)
				{
					foreach (AnnotationDesc annotation in annotations)
					{
						string qualifiedTypeName = annotation.AnnotationType().QualifiedTypeName();
						if (qualifiedTypeName.Equals(typeof(InterfaceAudience.Private).GetCanonicalName()
							) || qualifiedTypeName.Equals(typeof(InterfaceAudience.LimitedPrivate).GetCanonicalName
							()))
						{
							return true;
						}
						if (stability.Equals(StabilityOptions.EvolvingOption))
						{
							if (qualifiedTypeName.Equals(typeof(InterfaceStability.Unstable).GetCanonicalName
								()))
							{
								return true;
							}
						}
						if (stability.Equals(StabilityOptions.StableOption))
						{
							if (qualifiedTypeName.Equals(typeof(InterfaceStability.Unstable).GetCanonicalName
								()) || qualifiedTypeName.Equals(typeof(InterfaceStability.Evolving).GetCanonicalName
								()))
							{
								return true;
							}
						}
					}
					foreach (AnnotationDesc annotation_1 in annotations)
					{
						string qualifiedTypeName = annotation_1.AnnotationType().QualifiedTypeName();
						if (qualifiedTypeName.Equals(typeof(InterfaceAudience.Public).GetCanonicalName()))
						{
							return false;
						}
					}
				}
				if (treatUnannotatedClassesAsPrivate)
				{
					return doc.IsClass() || doc.IsInterface() || doc.IsAnnotationType();
				}
				return false;
			}

			private static object[] Filter(Doc[] array, Type componentType)
			{
				if (array == null || array.Length == 0)
				{
					return array;
				}
				IList<object> list = new AList<object>(array.Length);
				foreach (Doc entry in array)
				{
					if (!Exclude(entry))
					{
						list.AddItem(Process(entry, componentType));
					}
				}
				return Collections.ToArray(list, (object[])System.Array.CreateInstance(componentType
					, list.Count));
			}

			private object Unwrap(object proxy)
			{
				if (proxy is Proxy)
				{
					return ((RootDocProcessor.ExcludeHandler)Proxy.GetInvocationHandler(proxy)).target;
				}
				return proxy;
			}

			private bool IsFiltered(object[] args)
			{
				return args != null && true.Equals(args[0]);
			}
		}
	}
}
