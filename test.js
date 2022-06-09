const t = [1, 2, 3, 4, 5, 6, 7];

for (const d of t) {
  if (d > 4) {
    continue;
  }
  console.log(d);
}
