"""R10机器合同仍绑定的精简DayPlan路径兼容导出。

合同R11更新day_plan_pure_kernel绑定后即可删除本兼容模块。
"""

from stock_a.application.planning import assemble_day_plan

__all__ = ["assemble_day_plan"]
