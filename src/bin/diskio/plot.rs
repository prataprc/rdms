use std::path;
use std::time;

use plotters::prelude::*;

pub fn latency(
    path: path::PathBuf,
    title: String,
    mut values: Vec<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("plotting latency graph {}", title);

    let root = BitMapBackend::new(&path, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    let (xmin, xmax) = (0_u64, values.len() as u64);
    let (ymin, ymax) = (0_u64, values.iter().max().cloned().unwrap_or(0));
    let mut scatter_ctx = ChartBuilder::on(&root)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .margin(10)
        .caption(&title, ("Arial", 30).into_font())
        .build_cartesian_2d(xmin..xmax, ymin..ymax)?;
    scatter_ctx
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .label_style(("Arial", 15).into_font())
        .x_desc("N")
        .y_desc("Millisecond")
        .axis_desc_style(("Arial", 20).into_font())
        .draw()?;
    scatter_ctx.draw_series(
        values
            .iter()
            .enumerate()
            .map(|(i, l)| Circle::new((i as u64, *l), 2, RED.filled())),
    )?;

    values.sort();
    let off = (values.len() as f64 * 0.99) as usize;
    let p99 = time::Duration::from_nanos(values[off] * 1000);
    println!("99th percentile latency: {:?}", p99);
    Ok(())
}

pub fn throughput(
    path: path::PathBuf,
    title: String,
    mut values: Vec<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("plotting throughput graph {}", title);

    values.insert(0, 0);
    let throughputs: Vec<f64> = values
        .clone()
        .into_iter()
        .map(|x| (x as f64) / (1024_f64 * 1024_f64))
        .collect();

    let root = BitMapBackend::new(&path, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    let (xmin, xmax) = (0_u64, values.len() as u64);
    let (ymin, ymax) = (0_f64, values.iter().max().cloned().unwrap_or(0) as f64);
    let ymax = ymax / (1024_f64 * 1024_f64);
    let ymax = ymax + (ymax / 3_f64);
    let mut cc = ChartBuilder::on(&root)
        .x_label_area_size(40_i32)
        .y_label_area_size(60_i32)
        .margin(10_i32)
        .caption(&title, ("Arial", 30).into_font())
        .build_cartesian_2d(xmin..xmax, ymin..ymax)?;

    cc.configure_mesh()
        .bold_line_style(&WHITE)
        .label_style(("Arial", 15).into_font())
        .x_desc("Seconds")
        .y_desc("Throughput MB/sec")
        .axis_desc_style(("Arial", 20).into_font())
        .draw()?;

    cc.draw_series(LineSeries::new(
        throughputs
            .into_iter()
            .enumerate()
            .map(|(i, value)| (i as u64, value)),
        &RED,
    ))?;

    Ok(())
}
