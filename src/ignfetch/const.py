CODE_INSEE = (
    set()
    .union({str(i).zfill(3) for i in range(1, 20)})
    .union({str(i).zfill(3) for i in range(21, 96)})
    .union({str(i) for i in range(971, 987)})
    .union({"02A", "02B"})
)
