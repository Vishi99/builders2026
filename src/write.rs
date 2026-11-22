/// Each worker writes its own user_daily part; thread 0 then writes the
/// campaign table from all workers' totals.
fn phase_write(sh: &Shared, tid: usize, outdir: &Path) -> std::io::Result<()> {
    let ud = outdir.join("user_daily");
    let cd = outdir.join("campaign_daily");
    if tid == 0 {
        std::fs::create_dir_all(&ud)?;
        std::fs::create_dir_all(&cd)?;
    }
    sh.barrier.wait();
    let longtab = unsafe { sh.longtab.get().as_ref().unwrap() };
    let dims = &sh.dims;
    let nt = sh.nt;
    let t0 = sys::now();
    let rows: &[URow] = &sh.local(tid).out.as_ref().unwrap().rows;
    if !rows.is_empty() || tid == 0 {
        let n = rows.len();
        let mut w = pq::Writer::new(n, n * 16 + 65536);
        // user_id: one dictionary entry per run of rows with the same person.
        let mut dict = Vec::with_capacity(n * 6);
        let mut ids = Vec::with_capacity(n);
        let mut runs = 0u32;
        for i in 0..n {
            if i == 0 || rows[i].pkey != rows[i - 1].pkey {
                let at = dict.len();
                dict.extend_from_slice(&[0u8; 4]);
                longtab.bytes(rows[i].pkey, &mut dict);
                let l = (dict.len() - at - 4) as u32;
                dict[at..at + 4].copy_from_slice(&l.to_le_bytes());
                runs += 1;
            }
            ids.push(runs - 1);
        }
        w.dict_raw("user_id", 6, Some(0), &dict, runs as usize, &ids);
        let day: Vec<i32> = rows.iter().map(|r| r.day).collect();
        w.small_i32("day", true, &day);
        let v: Vec<i32> = rows.iter().map(|r| r.sessions as i32).collect();
        w.small_i32("sessions", false, &v);
        let v: Vec<i32> = rows.iter().map(|r| r.events as i32).collect();
        w.small_i32("events", false, &v);
        if rows.iter().all(|r| r.revenue <= i32::MAX as u64) {
            w.i32_col("revenue_cents", false, rows.iter().map(|r| r.revenue as i32));
        } else {
            w.i64_col("revenue_cents", rows.iter().map(|r| r.revenue as i64));
        }
        let cats: Vec<u32> = rows.iter().map(|r| r.cat).collect();
        w.dict_col("top_category", &cats, &dims.categories[1..], true);
        let bytes = w.finish();
        let t1 = sys::now();
        pq::write(&ud.join(format!("part-{tid:03}.parquet")), &bytes)?;
        if std::env::var("SS_TIMING").is_ok() {
            eprintln!("W t{tid}: rows {} encode {:.1} ms write {:.1} ms", rows.len(), (t1 - t0).as_secs_f64() * 1e3, t1.elapsed().as_secs_f64() * 1e3);
        }
    }
    if tid != nt - 1 {
        return Ok(());
    }
    // Campaign table (on the last worker, so thread 0 is not serial).
    let ncamp = dims.campaign_ids.len();
    let ndays = sh.local(0).out.as_ref().unwrap().ndays;
    let day0 = sh.local(0).out.as_ref().unwrap().day0;
    let mut tot = vec![sess::CampStat::default(); ncamp * ndays];
    for t in 0..nt {
        let o = sh.local(t).out.as_ref().unwrap();
        for (i, a) in tot.iter_mut().enumerate() {
            let b = &o.cred[i];
            a.sessions += b.sessions;
            a.purchasing += b.purchasing;
            a.revenue += b.revenue;
            a.assisted += o.assist[i];
        }
    }
    let mut chdict: Vec<Vec<u8>> = dims.channels.clone();
    chdict.sort();
    chdict.dedup();
    let mut sel: Vec<(usize, usize)> = Vec::new();
    for c in 0..ncamp {
        for d in 0..ndays {
            let st = &tot[d * ncamp + c];
            if st.sessions != 0 || st.assisted != 0 {
                sel.push((c, d));
            }
        }
    }
    let n = sel.len();
    let total: usize = sel.iter().map(|&(c, _)| dims.campaign_ids[c].len()).sum();
    let mut w = pq::Writer::new(n, total + n * 48);
    w.str_col("campaign_id", total, |i, out| out.extend_from_slice(&dims.campaign_ids[sel[i].0]));
    let ch_ids: Vec<u32> =
        sel.iter().map(|&(c, _)| chdict.binary_search(&dims.channels[c]).unwrap() as u32 + 1).collect();
    w.dict_col("channel", &ch_ids, &chdict, false);
    w.i32_col("day", true, sel.iter().map(|&(_, d)| (day0 + d as i64) as i32));
    w.i64_col("sessions", sel.iter().map(|&(c, d)| tot[d * ncamp + c].sessions as i64));
    w.i64_col("purchasing_sessions", sel.iter().map(|&(c, d)| tot[d * ncamp + c].purchasing as i64));
    w.i64_col("revenue_cents", sel.iter().map(|&(c, d)| tot[d * ncamp + c].revenue as i64));
    w.i64_col("assisted_sessions", sel.iter().map(|&(c, d)| tot[d * ncamp + c].assisted as i64));
    pq::write(&cd.join("part-000.parquet"), &w.finish())
}
