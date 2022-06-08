const aa = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

const index = aa.findIndex((item) => {
  console.log(item);
  return item.toString() === 4;
});

console.log(index);
