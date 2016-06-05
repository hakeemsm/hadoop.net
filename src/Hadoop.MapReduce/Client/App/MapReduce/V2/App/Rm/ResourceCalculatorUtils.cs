using System;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public class ResourceCalculatorUtils
	{
		public static int DivideAndCeil(int a, int b)
		{
			if (b == 0)
			{
				return 0;
			}
			return (a + (b - 1)) / b;
		}

		public static int ComputeAvailableContainers(Resource available, Resource required
			, EnumSet<YarnServiceProtos.SchedulerResourceTypes> resourceTypes)
		{
			if (resourceTypes.Contains(YarnServiceProtos.SchedulerResourceTypes.Cpu))
			{
				return Math.Min(available.GetMemory() / required.GetMemory(), available.GetVirtualCores
					() / required.GetVirtualCores());
			}
			return available.GetMemory() / required.GetMemory();
		}

		public static int DivideAndCeilContainers(Resource required, Resource factor, EnumSet
			<YarnServiceProtos.SchedulerResourceTypes> resourceTypes)
		{
			if (resourceTypes.Contains(YarnServiceProtos.SchedulerResourceTypes.Cpu))
			{
				return Math.Max(DivideAndCeil(required.GetMemory(), factor.GetMemory()), DivideAndCeil
					(required.GetVirtualCores(), factor.GetVirtualCores()));
			}
			return DivideAndCeil(required.GetMemory(), factor.GetMemory());
		}
	}
}
