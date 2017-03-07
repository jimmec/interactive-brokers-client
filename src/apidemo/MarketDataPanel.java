/* Copyright (C) 2013 Interactive Brokers LLC. All rights reserved.  This code is subject to the terms
 * and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable. */

package apidemo;

import java.awt.BorderLayout;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.client.ScannerSubscription;
import com.ib.client.Types.BarSize;
import com.ib.client.Types.DeepSide;
import com.ib.client.Types.DeepType;
import com.ib.client.Types.DurationUnit;
import com.ib.client.Types.MktDataType;
import com.ib.client.Types.WhatToShow;
import com.ib.controller.ApiController.IDeepMktDataHandler;
import com.ib.controller.ApiController.IHistoricalDataHandler;
import com.ib.controller.ApiController.IRealTimeBarHandler;
import com.ib.controller.ApiController.IScannerHandler;
import com.ib.controller.ApiController.ISecDefOptParamsReqHandler;
import com.ib.controller.Bar;
import com.ib.controller.Instrument;
import com.ib.controller.ScanCode;

import apidemo.util.HtmlButton;
import apidemo.util.NewTabbedPanel;
import apidemo.util.NewTabbedPanel.NewTabPanel;
import apidemo.util.TCombo;
import apidemo.util.UpperField;
import apidemo.util.VerticalPanel;
import apidemo.util.VerticalPanel.StackPanel;

public class MarketDataPanel extends JPanel {
	private final Contract m_contract = new Contract();
	private final NewTabbedPanel m_requestPanel = new NewTabbedPanel();
	private final NewTabbedPanel m_resultsPanel = new NewTabbedPanel();
	private TopResultsPanel m_topResultPanel;
	private ApiDemo.Logger logger;
	private Semaphore downloadRequestMutex;

	MarketDataPanel(Semaphore downloadRequestMutex, ApiDemo.Logger logger) {
		this(downloadRequestMutex);
		this.logger = logger;
	}
	MarketDataPanel(Semaphore downloadRequestMutex) {
		this.downloadRequestMutex = downloadRequestMutex;
		m_requestPanel.addTab( "Top Market Data", new TopRequestPanel() );
		m_requestPanel.addTab( "Deep Book", new DeepRequestPanel() );
		m_requestPanel.addTab( "Historical Data", new HistRequestPanel(downloadRequestMutex) );
		m_requestPanel.addTab( "Real-time Bars", new RealtimeRequestPanel() );
		m_requestPanel.addTab( "Market Scanner", new ScannerRequestPanel() );
		m_requestPanel.addTab("Security defininition optional parameters", new SecDefOptParamsPanel());
		
		setLayout( new BorderLayout() );
		add( m_requestPanel, BorderLayout.NORTH);
		add( m_resultsPanel);
	}
	
	private class TopRequestPanel extends JPanel {
		final ContractPanel m_contractPanel = new ContractPanel(m_contract);
		
		TopRequestPanel() {
			HtmlButton reqTop = new HtmlButton( "Request Top Market Data") {
				@Override protected void actionPerformed() {
					onTop();
				}
			};
			
			VerticalPanel butPanel = new VerticalPanel();
			butPanel.add( reqTop);
			
			setLayout( new BoxLayout( this, BoxLayout.X_AXIS) );
			add( m_contractPanel);
			add( Box.createHorizontalStrut(20));
			add( butPanel);
		}

		protected void onTop() {
			m_contractPanel.onOK();
			if (m_topResultPanel == null) {
				m_topResultPanel = new TopResultsPanel();
				m_resultsPanel.addTab( "Top Data", m_topResultPanel, true, true);
			}
			
			m_topResultPanel.m_model.addRow( m_contract);
		}
	}
	
	private class TopResultsPanel extends NewTabPanel {
		final TopModel m_model = new TopModel();
		final JTable m_tab = new TopTable( m_model);
		final TCombo<MktDataType> m_typeCombo = new TCombo<MktDataType>( MktDataType.values() );

		TopResultsPanel() {
			m_typeCombo.removeItemAt( 0);

			JScrollPane scroll = new JScrollPane( m_tab);

			HtmlButton reqType = new HtmlButton( "Go") {
				@Override protected void actionPerformed() {
					onReqType();
				}
			};

			VerticalPanel butPanel = new VerticalPanel();
			butPanel.add( "Market data type", m_typeCombo, reqType);
			
			setLayout( new BorderLayout() );
			add( scroll);
			add( butPanel, BorderLayout.SOUTH);
		}
		
		/** Called when the tab is first visited. */
		@Override public void activated() {
		}

		/** Called when the tab is closed by clicking the X. */
		@Override public void closed() {
			m_model.desubscribe();
			m_topResultPanel = null;
		}

		void onReqType() {
			ApiDemo.INSTANCE.controller().reqMktDataType( m_typeCombo.getSelectedItem() );
		}
		
		class TopTable extends JTable {
			public TopTable(TopModel model) { super( model); }

			@Override public TableCellRenderer getCellRenderer(int rowIn, int column) {
				TableCellRenderer rend = super.getCellRenderer(rowIn, column);
				m_model.color( rend, rowIn, getForeground() );
				return rend;
			}
		}
	}		
	
	private class DeepRequestPanel extends JPanel {
		final ContractPanel m_contractPanel = new ContractPanel(m_contract);
		
		DeepRequestPanel() {
			HtmlButton reqDeep = new HtmlButton( "Request Deep Market Data") {
				@Override protected void actionPerformed() {
					onDeep();
				}
			};
			
			VerticalPanel butPanel = new VerticalPanel();
			butPanel.add( reqDeep);
			
			setLayout( new BoxLayout( this, BoxLayout.X_AXIS) );
			add( m_contractPanel);
			add( Box.createHorizontalStrut(20));
			add( butPanel);
		}

		protected void onDeep() {
			m_contractPanel.onOK();
			DeepResultsPanel resultPanel = new DeepResultsPanel();
			m_resultsPanel.addTab( "Deep " + m_contract.symbol(), resultPanel, true, true);
			ApiDemo.INSTANCE.controller().reqDeepMktData(m_contract, 6, resultPanel);
		}
	}

	private static class DeepResultsPanel extends NewTabPanel implements IDeepMktDataHandler {
		final DeepModel m_buy = new DeepModel();
		final DeepModel m_sell = new DeepModel();

		DeepResultsPanel() {
			HtmlButton desub = new HtmlButton( "Desubscribe") {
				public void actionPerformed() {
					onDesub();
				}
			};
			
			JTable buyTab = new JTable( m_buy);
			JTable sellTab = new JTable( m_sell);
			
			JScrollPane buyScroll = new JScrollPane( buyTab);
			JScrollPane sellScroll = new JScrollPane( sellTab);
			
			JPanel mid = new JPanel( new GridLayout( 1, 2) );
			mid.add( buyScroll);
			mid.add( sellScroll);
			
			setLayout( new BorderLayout() );
			add( mid);
			add( desub, BorderLayout.SOUTH);
		}
		
		protected void onDesub() {
			ApiDemo.INSTANCE.controller().cancelDeepMktData( this);
		}

		@Override public void activated() {
		}

		/** Called when the tab is closed by clicking the X. */
		@Override public void closed() {
			ApiDemo.INSTANCE.controller().cancelDeepMktData( this);
		}
		
		@Override public void updateMktDepth(int pos, String mm, DeepType operation, DeepSide side, double price, int size) {
			if (side == DeepSide.BUY) {
				m_buy.updateMktDepth(pos, mm, operation, price, size);
			}
			else {
				m_sell.updateMktDepth(pos, mm, operation, price, size);
			}
		}

		class DeepModel extends AbstractTableModel {
			final ArrayList<DeepRow> m_rows = new ArrayList<DeepRow>();

			@Override public int getRowCount() {
				return m_rows.size();
			}

			public void updateMktDepth(int pos, String mm, DeepType operation, double price, int size) {
				switch( operation) {
					case INSERT:
						m_rows.add( pos, new DeepRow( mm, price, size) );
						fireTableRowsInserted(pos, pos);
						break;
					case UPDATE:
						m_rows.get( pos).update( mm, price, size);
						fireTableRowsUpdated(pos, pos);
						break;
					case DELETE:
						if (pos < m_rows.size() ) {
							m_rows.remove( pos);
						}
						else {
							// this happens but seems to be harmless
							// System.out.println( "can't remove " + pos);
						}
						fireTableRowsDeleted(pos, pos);
						break;
				}
			}

			@Override public int getColumnCount() {
				return 3;
			}
			
			@Override public String getColumnName(int col) {
				switch( col) {
					case 0: return "Mkt Maker";
					case 1: return "Price";
					case 2: return "Size";
					default: return null;
				}
			}

			@Override public Object getValueAt(int rowIn, int col) {
				DeepRow row = m_rows.get( rowIn);
				
				switch( col) {
					case 0: return row.m_mm;
					case 1: return row.m_price;
					case 2: return row.m_size;
					default: return null;
				}
			}
		}
		
		static class DeepRow {
			String m_mm;
			double m_price;
			int m_size;

			public DeepRow(String mm, double price, int size) {
				update( mm, price, size);
			}
			
			void update( String mm, double price, int size) {
				m_mm = mm;
				m_price = price;
				m_size = size;
			}
		}
	}

	private class HistRequestPanel extends JPanel {
		final ContractPanel m_contractPanel = new ContractPanel(m_contract);
		final UpperField m_end = new UpperField();
		final UpperField m_duration = new UpperField();
		final TCombo<DurationUnit> m_durationUnit = new TCombo<DurationUnit>( DurationUnit.values() );
		final TCombo<BarSize> m_barSize = new TCombo<BarSize>( BarSize.values() );
		final TCombo<WhatToShow> m_whatToShow = new TCombo<WhatToShow>( WhatToShow.values() );
		final JCheckBox m_rthOnly = new JCheckBox();
		final Semaphore downloadRequestMutex;
		final ScheduledExecutorService mutexTimeoutMonitorExecutor = Executors.newScheduledThreadPool(5);
		
		HistRequestPanel(Semaphore downloadRequestMutex) {
			this.downloadRequestMutex = downloadRequestMutex;
			m_end.setText( "20120101 12:00:00");
			m_duration.setText( "1");
			m_durationUnit.setSelectedItem( DurationUnit.WEEK);
			m_barSize.setSelectedItem( BarSize._1_hour);
			
			HtmlButton button = new HtmlButton( "Request historical data") {
				@Override protected void actionPerformed() {
					onHistorical();
				}
			};
			
	    	VerticalPanel paramPanel = new VerticalPanel();
			paramPanel.add( "End", m_end);
			paramPanel.add( "Duration", m_duration);
			paramPanel.add( "Duration unit", m_durationUnit);
			paramPanel.add( "Bar size", m_barSize);
			paramPanel.add( "What to show", m_whatToShow);
			paramPanel.add( "RTH only", m_rthOnly);
			
			VerticalPanel butPanel = new VerticalPanel();
			butPanel.add( button);
			
			JPanel rightPanel = new StackPanel();
			rightPanel.add( paramPanel);
			rightPanel.add( Box.createVerticalStrut( 20));
			rightPanel.add( butPanel);
			
			setLayout( new BoxLayout( this, BoxLayout.X_AXIS) );
			add( m_contractPanel);
			add( Box.createHorizontalStrut(20) );
			add( rightPanel);
		}
	
		protected void onHistorical() {
			m_contractPanel.onOK();
//			BarResultsPanel panel = new BarResultsPanel(true, m_contract, m_end.getText(), m_duration.getInt(),
//					m_durationUnit.getSelectedItem(), m_barSize.getSelectedItem(), m_whatToShow.getSelectedItem(), downloadRequestMutex);
//			ApiDemo.INSTANCE.controller()
//					.reqHistoricalData(m_contract, m_end.getText(), m_duration.getInt(),
//							m_durationUnit.getSelectedItem(), m_barSize.getSelectedItem(),
//							m_whatToShow.getSelectedItem(), m_rthOnly.isSelected(), panel);
//			m_resultsPanel.addTab("Historical " + m_contract.symbol(), panel, true, true);
            downloadAllTickers();
		}

		private void downloadAllTickers() {
			List<Long> previousDownloadTimeSeconds = new ArrayList<>(505);
			String[] tickers = new String[] {"MMM","ABT","ABBV","ACN","ATVI","AYI","ADBE","AAP","AES","AET","AMG","AFL","A","APD","AKAM","ALK","ALB","AGN","LNT","ALXN","ALLE","ADS","ALL","GOOGL","GOOG","MO","AMZN","AEE","AAL","AEP","AXP","AIG","AMT","AWK","AMP","ABC","AME","AMGN","APH","APC","ADI","ANTM","AON","APA","AIV","AAPL","AMAT","ADM","ARNC","AJG","AIZ","T","ADSK","ADP","AN","AZO","AVB","AVY","BHI","BLL","BAC","BK","BCR","BAX","BBT","BDX","BBBY","BRK B","BBY","BIIB","BLK","HRB","BA","BWA","BXP","BSX","BMY","AVGO","BF B","CHRW","CA","COG","CPB","COF","CAH","HSIC","KMX","CCL","CAT","CBG","CBS","CELG","CNC","CNP","CTL","CERN","CF","SCHW","CHTR","CHK","CVX","CMG","CB","CHD","CI","XEC","CINF","CTAS","CSCO","C","CFG","CTXS","CLX","CME","CMS","COH","KO","CTSH","CL","CMCSA","CMA","CAG","CXO","COP","ED","STZ","GLW","COST","COTY","CCI","CSRA","CSX","CMI","CVS","DHI","DHR","DRI","DVA","DE","DLPH","DAL","XRAY","DVN","DLR","DFS","DISCA","DISCK","DG","DLTR","D","DOV","DOW","DPS","DTE","DD","DUK","DNB","ETFC","EMN","ETN","EBAY","ECL","EIX","EW","EA","EMR","ENDP","ETR","EVHC","EOG","EQT","EFX","EQIX","EQR","ESS","EL","ES","EXC","EXPE","EXPD","ESRX","EXR","XOM","FFIV","FB","FAST","FRT","FDX","FIS","FITB","FSLR","FE","FISV","FLIR","FLS","FLR","FMC","FTI","FL","F","FTV","FBHS","BEN","FCX","FTR","GPS","GRMN","GD","GE","GGP","GIS","GM","GPC","GILD","GPN","GS","GT","GWW","HAL","HBI","HOG","HAR","HRS","HIG","HAS","HCA","HCP","HP","HES","HPE","HOLX","HD","HON","HRL","HST","HPQ","HUM","HBAN","IDXX","ITW","ILMN","IR","INTC","ICE","IBM","IP","IPG","IFF","INTU","ISRG","IVZ","IRM","JEC","JBHT","SJM","JNJ","JCI","JPM","JNPR","KSU","K","KEY","KMB","KIM","KMI","KLAC","KSS","KHC","KR","LB","LLL","LH","LRCX","LEG","LEN","LVLT","LUK","LLY","LNC","LLTC","LKQ","LMT","L","LOW","LYB","MTB","MAC","M","MNK","MRO","MPC","MAR","MMC","MLM","MAS","MA","MAT","MKC","MCD","MCK","MJN","MDT","MRK","MET","MTD","KORS","MCHP","MU","MSFT","MAA","MHK","TAP","MDLZ","MON","MNST","MCO","MS","MOS","MSI","MUR","MYL","NDAQ","NOV","NAVI","NTAP","NFLX","NWL","NFX","NEM","NWSA","NWS","NEE","NLSN","NKE","NI","NBL","JWN","NSC","NTRS","NOC","NRG","NUE","NVDA","ORLY","OXY","OMC","OKE","ORCL","PCAR","PH","PDCO","PAYX","PYPL","PNR","PBCT","PEP","PKI","PRGO","PFE","PCG","PM","PSX","PNW","PXD","PBI","PNC","RL","PPG","PPL","PX","PCLN","PFG","PG","PGR","PLD","PRU","PEG","PSA","PHM","PVH","QRVO","PWR","QCOM","DGX","RRC","RTN","O","RHT","REGN","RF","RSG","RAI","RHI","ROK","COL","ROP","ROST","RCL","R","CRM","SCG","SLB","SNI","STX","SEE","SRE","SHW","SIG","SPG","SWKS","SLG","SNA","SO","LUV","SWN","SE","SPGI","SWK","SPLS","SBUX","STT","SRCL","SYK","STI","SYMC","SYF","SYY","TROW","TGT","TEL","TGNA","TDC","TSO","TXN","TXT","COO","HSY","TRV","TMO","TIF","TWX","TJX","TMK","TSS","TSCO","TDG","RIG","TRIP","FOXA","FOX","TSN","UDR","ULTA","USB","UA","UAA","UNP","UAL","UNH","UPS","URI","UTX","UHS","UNM","URBN","VFC","VLO","VAR","VTR","VRSN","VRSK","VZ","VRTX","VIAB","V","VNO","VMC","WMT","WBA","DIS","WM","WAT","WEC","WFC","HCN","WDC","WU","WRK","WY","WHR","WFM","WMB","WLTW","WYN","WYNN","XEL","XRX","XLNX","XL","XYL","YHOO","YUM","ZBH","ZION","ZTS"}; int waitSeconds = 35;
			Arrays.sort(tickers);
			for (int i = 0; i < tickers.length; ++i) {
				Instant starttime = Instant.now();
				try {
					downloadRequestMutex.acquire();
				} catch (InterruptedException e) {
				    System.out.println("interrupted acquiring mutex " + e.getMessage());
					e.printStackTrace();
				}
				Instant endtime = Instant.now(); // this is the endtime as acquiring mutex again means last download finished;
				previousDownloadTimeSeconds.add(starttime.until(endtime, ChronoUnit.SECONDS)); // so this is the time it took for previous download

				String ticker = tickers[i];
				String currProgressStat = (i + 1) + " out of " + tickers.length;
				String statUpdate = Instant.now().toString() + " - Downloading [" + ticker + "], " + currProgressStat;
				System.out.println(statUpdate);

				m_contract.symbol(ticker);

				BarResultsPanel panel = new BarResultsPanel(true, m_contract, m_end.getText(), m_duration.getInt(),
						m_durationUnit.getSelectedItem(), m_barSize.getSelectedItem(), m_whatToShow.getSelectedItem(),
						downloadRequestMutex, mutexTimeoutMonitorExecutor);
				ApiDemo.INSTANCE.controller()
						.reqHistoricalData(m_contract, m_end.getText(), m_duration.getInt(),
								m_durationUnit.getSelectedItem(), m_barSize.getSelectedItem(),
								m_whatToShow.getSelectedItem(), m_rthOnly.isSelected(), panel);
//				m_resultsPanel.addTab( "Historical " + m_contract.symbol(), panel, true, true);

				String waitMsg = Instant.now().toString() + " - Waiting until all data received...";
				System.out.println(waitMsg);
				if (i==0) previousDownloadTimeSeconds.clear(); // delete first round times
				System.out.println("Timing data so far: " + previousDownloadTimeSeconds.stream().mapToLong(Long::new).summaryStatistics().toString());
				System.out.println(previousDownloadTimeSeconds);
			}
		}
	}

	private class RealtimeRequestPanel extends JPanel {
		final ContractPanel m_contractPanel = new ContractPanel(m_contract);
		final TCombo<WhatToShow> m_whatToShow = new TCombo<WhatToShow>( WhatToShow.values() );
		final JCheckBox m_rthOnly = new JCheckBox();
		
		RealtimeRequestPanel() { 		
			HtmlButton button = new HtmlButton( "Request real-time bars") {
				@Override protected void actionPerformed() {
					onRealTime();
				}
			};
	
	    	VerticalPanel paramPanel = new VerticalPanel();
			paramPanel.add( "What to show", m_whatToShow);
			paramPanel.add( "RTH only", m_rthOnly);
			
			VerticalPanel butPanel = new VerticalPanel();
			butPanel.add( button);
			
			JPanel rightPanel = new StackPanel();
			rightPanel.add( paramPanel);
			rightPanel.add( Box.createVerticalStrut( 20));
			rightPanel.add( butPanel);
			
			setLayout( new BoxLayout( this, BoxLayout.X_AXIS) );
			add( m_contractPanel);
			add( Box.createHorizontalStrut(20) );
			add( rightPanel);
		}
	
		protected void onRealTime() {
			m_contractPanel.onOK();
			BarResultsPanel panel = new BarResultsPanel( false);
			ApiDemo.INSTANCE.controller().reqRealTimeBars(m_contract, m_whatToShow.getSelectedItem(), m_rthOnly.isSelected(), panel);
			m_resultsPanel.addTab( "Real-time " + m_contract.symbol(), panel, true, true);
		}
	}
	
	static class BarResultsPanel extends NewTabPanel implements IHistoricalDataHandler, IRealTimeBarHandler {
		final BarModel m_model = new BarModel();
		final ArrayList<Bar> m_rows = new ArrayList<Bar>();
		final boolean m_historical;
		final Chart m_chart = new Chart( m_rows);
		FileWriter outputFileWriter;
		Contract contract;
		String endDate;
		int duration;
		DurationUnit durationUnit;
		BarSize barSize;
		WhatToShow whatToShow;
		String path;
		Semaphore downloadRequestMutex;
		ScheduledExecutorService mutexTimeoutMonitorExecutor;

		/**
		 * Stores all params used to make a historical data request so it saves them in a csv dump
		 * @param historical
		 * @param contract
		 * @param endDate
		 * @param duration
		 * @param durationUnit
		 * @param barSize
		 * @param whatToShow
		 */
		BarResultsPanel(boolean historical, Contract contract, String endDate, int duration, DurationUnit durationUnit, BarSize barSize, WhatToShow whatToShow, Semaphore mutex, ScheduledExecutorService mutexTimeoutMonitorExecutor) {
			this(historical);
			this.contract = contract;
			this.endDate = endDate;
			this.duration = duration;
			this.durationUnit = durationUnit;
			this.barSize = barSize;
			this.whatToShow = whatToShow;
			this.downloadRequestMutex = mutex;
			this.mutexTimeoutMonitorExecutor = mutexTimeoutMonitorExecutor;

			this.path = String.format("/Users/jimmy/price-data/sp500-30min/2007-2012/%s-%s-%s%s-%s.csv",
					contract.symbol(), endDate, duration, durationUnit, barSize.toString());
			try {
				this.outputFileWriter = new FileWriter(new File(path));
			} catch (IOException e) {
			    System.out.println("could not open file at: " + path + " for saving data. Due to: " + e.getMessage());
			}
			// Since we don't seem to catch exceptions from the API correctly
			// install a background thread to implement a timeout so that we release the mutex after X mins in case the
			// Symbol was bad or otherwise unavailable
			int timeout = 3;
			mutexTimeoutMonitorExecutor.schedule(() -> {
				System.out.println("NOTE: " + this.path + " timed out after " + timeout + " mins, releasing lock.");
				mutex.release();
			}, timeout, TimeUnit.MINUTES);
		}
		
		BarResultsPanel( boolean historical) {
			m_historical = historical;
			
			JTable tab = new JTable( m_model);
			JScrollPane scroll = new JScrollPane( tab) {
				public Dimension getPreferredSize() {
					Dimension d = super.getPreferredSize();
					d.width = 500;
					return d;
				}
			};

			JScrollPane chartScroll = new JScrollPane( m_chart);

			setLayout( new BorderLayout() );
			add( scroll, BorderLayout.WEST);
			add( chartScroll, BorderLayout.CENTER);
		}

		/** Called when the tab is first visited. */
		@Override public void activated() {
		}

		/** Called when the tab is closed by clicking the X. */
		@Override public void closed() {
			if (m_historical) {
				ApiDemo.INSTANCE.controller().cancelHistoricalData( this);
			}
			else {
				ApiDemo.INSTANCE.controller().cancelRealtimeBars( this);
			}
		}

		@Override public void historicalData(Bar bar, boolean hasGaps) {
			m_rows.add( bar);
		}
		
		@Override public void historicalDataEnd() {
			System.out.println("============================");
			System.out.println(Instant.now().toString() + " - Received full set of data, writing to: " + path);
			try {
				outputFileWriter.write(String.format("%s,%s,%s,%s,%s,%s", "timestamp", "open", "close", "high", "low", "vol"));
				outputFileWriter.write("\n");
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
			for (Bar b : m_rows) {
				try {
					outputFileWriter.write(String.format("%s,%s,%s,%s,%s,%s",
                            b.formattedTime(), b.open(), b.close(), b.high(), b.low(), b.volume()));
					outputFileWriter.write("\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				outputFileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			m_rows.clear(); // clear memory after its all written to disk;
			downloadRequestMutex.release();
			System.out.println(Instant.now().toString() + " - Written and released Mutex");
			System.out.println("============================");
//			fire();
		}

		@Override public void realtimeBar(Bar bar) {
			m_rows.add( bar); 
			fire();
		}
		
		private void fire() {
			SwingUtilities.invokeLater( new Runnable() {
				@Override public void run() {
					m_model.fireTableRowsInserted( m_rows.size() - 1, m_rows.size() - 1);
					m_chart.repaint();
				}
			});
		}

		class BarModel extends AbstractTableModel {
			@Override public int getRowCount() {
				return m_rows.size();
			}

			@Override public int getColumnCount() {
				return 7;
			}
			
			@Override public String getColumnName(int col) {
				switch( col) {
					case 0: return "Date/time";
					case 1: return "Open";
					case 2: return "High";
					case 3: return "Low";
					case 4: return "Close";
					case 5: return "Volume";
					case 6: return "WAP";
					default: return null;
				}
			}

			@Override public Object getValueAt(int rowIn, int col) {
				Bar row = m_rows.get( rowIn);
				switch( col) {
					case 0: return row.formattedTime();
					case 1: return row.open();
					case 2: return row.high();
					case 3: return row.low();
					case 4: return row.close();
					case 5: return row.volume();
					case 6: return row.wap();
					default: return null;
				}
			}
		}		
	}
	
	private class ScannerRequestPanel extends JPanel {
		final UpperField m_numRows = new UpperField( "15");
		final TCombo<ScanCode> m_scanCode = new TCombo<ScanCode>( ScanCode.values() );
		final TCombo<Instrument> m_instrument = new TCombo<Instrument>( Instrument.values() );
		final UpperField m_location = new UpperField( "STK.US.MAJOR", 9);
		final TCombo<String> m_stockType = new TCombo<String>( "ALL", "STOCK", "ETF");
		
		ScannerRequestPanel() {
			HtmlButton go = new HtmlButton( "Go") {
				@Override protected void actionPerformed() {
					onGo();
				}
			};
			
			VerticalPanel paramsPanel = new VerticalPanel();
			paramsPanel.add( "Scan code", m_scanCode);
			paramsPanel.add( "Instrument", m_instrument);
			paramsPanel.add( "Location", m_location, Box.createHorizontalStrut(10), go);
			paramsPanel.add( "Stock type", m_stockType);
			paramsPanel.add( "Num rows", m_numRows);
			
			setLayout( new BorderLayout() );
			add( paramsPanel, BorderLayout.NORTH);
		}

		protected void onGo() {
			ScannerSubscription sub = new ScannerSubscription();
			sub.numberOfRows( m_numRows.getInt() );
			sub.scanCode( m_scanCode.getSelectedItem().toString() );
			sub.instrument( m_instrument.getSelectedItem().toString() );
			sub.locationCode( m_location.getText() );
			sub.stockTypeFilter( m_stockType.getSelectedItem().toString() );
			
			ScannerResultsPanel resultsPanel = new ScannerResultsPanel();
			m_resultsPanel.addTab( sub.scanCode(), resultsPanel, true, true);

			ApiDemo.INSTANCE.controller().reqScannerSubscription( sub, resultsPanel);
		}
	}

	static class ScannerResultsPanel extends NewTabPanel implements IScannerHandler {
		final HashSet<Integer> m_conids = new HashSet<Integer>();
		final TopModel m_model = new TopModel();

		ScannerResultsPanel() {
			JTable table = new JTable( m_model);
			JScrollPane scroll = new JScrollPane( table);
			setLayout( new BorderLayout() );
			add( scroll);
		}

		/** Called when the tab is first visited. */
		@Override public void activated() {
		}

		/** Called when the tab is closed by clicking the X. */
		@Override public void closed() {
			ApiDemo.INSTANCE.controller().cancelScannerSubscription( this);
			m_model.desubscribe();
		}

		@Override public void scannerParameters(String xml) {
			try {
				File file = File.createTempFile( "pre", ".xml");
				FileWriter writer = new FileWriter( file);
				writer.write( xml);
				writer.close();

				Desktop.getDesktop().open( file);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override public void scannerData(int rank, final ContractDetails contractDetails, String legsStr) {
			if (!m_conids.contains( contractDetails.conid() ) ) {
				m_conids.add( contractDetails.conid() );
				SwingUtilities.invokeLater( new Runnable() {
					@Override public void run() {
						m_model.addRow( contractDetails.contract() );
					}
				});
			}
		}

		@Override public void scannerDataEnd() {
			// we could sort here
		}
	}
	
	class SecDefOptParamsPanel extends JPanel {
		
		final UpperField m_underlyingSymbol = new UpperField();
		final UpperField m_futFopExchange = new UpperField();
//		final UpperField m_currency = new UpperField();
		final UpperField m_underlyingSecType = new UpperField();
		final UpperField m_underlyingConId = new UpperField();
		
		SecDefOptParamsPanel() {
			VerticalPanel paramsPanel = new VerticalPanel();
			HtmlButton go = new HtmlButton("Go") { @Override protected void actionPerformed() { onGo(); } };
			
			m_underlyingConId.setText(Integer.MAX_VALUE);			
			paramsPanel.add("Underlying symbol", m_underlyingSymbol);			
			paramsPanel.add("FUT-FOP exchange", m_futFopExchange);			
//			paramsPanel.add("Currency", m_currency);			
			paramsPanel.add("Underlying security type", m_underlyingSecType);			
			paramsPanel.add("Underlying contract id", m_underlyingConId);			
			paramsPanel.add(go);
			setLayout(new BorderLayout());
			add(paramsPanel, BorderLayout.NORTH);
		}

		protected void onGo() {
			String underlyingSymbol = m_underlyingSymbol.getText();
			String futFopExchange = m_futFopExchange.getText();
//			String currency = m_currency.getText();
			String underlyingSecType = m_underlyingSecType.getText();
			int underlyingConId = m_underlyingConId.getInt();
			
			ApiDemo.INSTANCE.controller().reqSecDefOptParams( 
					underlyingSymbol,
					futFopExchange,
//					currency,
					underlyingSecType,
					underlyingConId,
					new ISecDefOptParamsReqHandler() {
						
						@Override
						public void securityDefinitionOptionalParameterEnd(int reqId) { }
						
						@Override
						public void securityDefinitionOptionalParameter(final String exchange, final int underlyingConId, final String tradingClass, final String multiplier,
								final Set<String> expirations, final Set<Double> strikes) {
							SwingUtilities.invokeLater( new Runnable() { @Override public void run() {

								SecDefOptParamsReqResultsPanel resultsPanel = new SecDefOptParamsReqResultsPanel(expirations, strikes);

								m_resultsPanel.addTab(exchange + " " + 
										underlyingConId + " " + 
										tradingClass + " " + 
										multiplier, 
										resultsPanel, true, true);
							}});
						}
					});
		}
		
	}
	
	static class SecDefOptParamsReqResultsPanel extends NewTabPanel {

		final OptParamsModel m_model;
		
		public SecDefOptParamsReqResultsPanel(Set<String> expirations, Set<Double> strikes) {
			JTable table = new JTable(m_model = new OptParamsModel(expirations, strikes));
			JScrollPane scroll = new JScrollPane(table);
			
			setLayout(new BorderLayout());
			add(scroll);
		}
		
		@Override
		public void activated() {
		}

		@Override
		public void closed() {
		}

	}
}
