export async function requestD1Material(payload, signal) {
  const response = await fetch("/api/product/d1-material", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal,
  });
  const result = await response.json();
  if (!response.ok || result.ok === false) {
    throw new Error(
      `${result.code || response.status}：${result.message || "请求失败"}`,
    );
  }
  return result;
}
